"""Export reconciled aggregate Gold facts for the hosted analytics application.

Only controlled dimensions and additive counts are exported. Raw observations,
request identifiers, addresses, coordinates, and duration quantiles are excluded.
"""

import argparse
import gzip
import hashlib
import json
import os
import re
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

COUNTS = (
    "requests",
    "unresolved_now",
    "closed_missing_date",
    "nonclosed_with_date",
    "invalid_resolution_count",
)
MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def count_value(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return value


def build_snapshot(fact_rows, daily_rows, audit: dict, *, captured_at: str | None = None):
    """Build only the public schema from projected aggregate rows."""
    if audit.get("status") != "passed":
        raise ValueError("full-data audit must pass before public analytics export")
    expected = count_value(audit["silver_rows"], "audited source count")
    if not expected or audit.get("bronze_rows") != expected:
        raise ValueError("full audit must reconcile nonempty Bronze and Silver counts")
    rows = []
    keys = set()
    for raw in fact_rows:
        month, borough, category = raw["month"], raw["borough"], raw["complaint_type"]
        if not isinstance(month, str) or not MONTH.fullmatch(month):
            raise ValueError("fact month must be YYYY-MM")
        if not isinstance(borough, str) or not isinstance(category, str):
            raise ValueError("fact borough and category must be strings")
        key = month, borough, category
        if key in keys:
            raise ValueError("duplicate monthly borough/category fact")
        keys.add(key)
        values = [count_value(raw[name], name) for name in COUNTS]
        if any(value > values[0] for value in values[1:]):
            raise ValueError("quality count exceeds the corresponding request count")
        rows.append((key, values))
    daily = []
    days = set()
    for raw in daily_rows:
        day = raw["day"]
        if isinstance(day, date):
            day = day.isoformat()
        if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day:
            raise ValueError("daily date must be YYYY-MM-DD")
        if day in days:
            raise ValueError("duplicate daily demand date")
        days.add(day)
        daily.append([day, count_value(raw["requests"], "daily requests")])
    fact_total = sum(values[0] for _, values in rows)
    daily_total = sum(item[1] for item in daily)
    if fact_total != expected or daily_total != expected:
        raise ValueError(
            f"aggregate totals do not reconcile: facts={fact_total}, "
            f"daily={daily_total}, audit={expected}"
        )
    months = sorted({key[0] for key, _ in rows})
    boroughs = sorted({key[1] for key, _ in rows})
    categories = sorted({key[2] for key, _ in rows})
    indexes = [
        {value: i for i, value in enumerate(dimension)}
        for dimension in (months, boroughs, categories)
    ]
    facts = [
        [indexes[0][key[0]], indexes[1][key[1]], indexes[2][key[2]], *values]
        for key, values in sorted(rows)
    ]
    daily.sort()
    excluded_day = audit.get("excluded_latest_source_day") or audit["scope_end_exclusive"][:10]
    excluded_day = date.fromisoformat(excluded_day).isoformat()
    return {
        "schema_version": 1,
        "captured_at": captured_at or datetime.now(UTC).isoformat(),
        "source_start": daily[0][0],
        "source_end": daily[-1][0],
        "source_records": expected,
        "excluded_latest_source_day": excluded_day,
        "months": months,
        "boroughs": boroughs,
        "categories": categories,
        "facts": facts,
        "daily": daily,
        "semantics": (
            "Aggregate snapshot of count-reconciled source observations, not CDC. "
            "Unresolved counts reflect observed current status, not historical backlog. "
            "The latest source day is included in analytics and may be incomplete; "
            "it is excluded from model training."
        ),
    }


def atomic_bytes(path: Path, payload: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.part")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_snapshot(output: Path, snapshot: dict):
    payload = json.dumps(
        snapshot, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode()
    compressed = gzip.compress(payload, compresslevel=9, mtime=0)
    checksum = hashlib.sha256(compressed).hexdigest()
    atomic_bytes(output, compressed)
    atomic_bytes(output.with_name(output.name + ".sha256"), (checksum + "\n").encode())
    return {
        "output": str(output),
        "bytes": len(compressed),
        "sha256": checksum,
        "source_records": snapshot["source_records"],
        "facts": len(snapshot["facts"]),
        "daily_days": len(snapshot["daily"]),
        "months": len(snapshot["months"]),
        "boroughs": len(snapshot["boroughs"]),
        "categories": len(snapshot["categories"]),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake", type=Path, default=Path("data/full/lake"))
    parser.add_argument("--audit", type=Path, default=Path("artifacts/full-verification.json"))
    parser.add_argument("--output", type=Path, default=Path("artifacts/public-analytics.json.gz"))
    args = parser.parse_args()
    audit = json.loads(args.audit.read_text(encoding="utf-8"))
    if audit.get("status") != "passed":
        raise ValueError("full-data audit must pass before scanning Gold")
    from pyspark.sql import functions as F

    from urbanlake.processing import session

    spark = session()
    try:
        requests = spark.read.format("delta").load(str(args.lake / "gold/daily_requests"))
        grouped = (
            requests.withColumn("month", F.date_format("day", "yyyy-MM"))
            .groupBy("month", "borough", "complaint_type")
            .agg(*(F.sum(name).alias(name) for name in COUNTS))
        )
        daily = (
            spark.read.format("delta")
            .load(str(args.lake / "gold/daily_demand"))
            .select(F.date_format("day", "yyyy-MM-dd").alias("day"), "requests")
        )
        snapshot = build_snapshot(grouped.toLocalIterator(), daily.toLocalIterator(), audit)
        report = write_snapshot(args.output, snapshot)
        print(json.dumps(report, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
