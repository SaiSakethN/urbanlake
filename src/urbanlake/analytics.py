"""Read-only monthly analytics from the verified public aggregate artifact.

The hosted API reads aggregate counts only. It does not expose raw NYC requests,
connect to the local lakehouse, or reconstruct historical request transitions.
"""

import csv
import gzip
import io
import json
import os
import re
from collections import defaultdict
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response

router = APIRouter(prefix="/v1/analytics", tags=["Analytics"])
MONTH_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])$"
QUALITY = ("closed_missing_date", "nonclosed_with_date", "invalid_resolution_count")


def _integer(value):
    return type(value) is int and value >= 0


def _day(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("analytics dates must be YYYY-MM-DD")
    return date.fromisoformat(value)


def validate_artifact(data):
    """Reject inconsistent exported evidence instead of serving partial totals."""
    if not isinstance(data, dict) or data.get("schema_version") != 1:
        raise ValueError("unsupported public analytics schema")
    if not _integer(data.get("source_records")) or data["source_records"] == 0:
        raise ValueError("public analytics requires a positive audited source count")
    stamp = data.get("captured_at")
    if not isinstance(stamp, str) or datetime.fromisoformat(stamp).tzinfo is None:
        raise ValueError("analytics capture timestamp must be timezone aware")
    first, last = _day(data["source_start"]), _day(data["source_end"])
    partial = _day(data["excluded_latest_source_day"])
    if not first <= partial <= last:
        raise ValueError("analytics source coverage/partial-day metadata is inconsistent")
    if not isinstance(data.get("semantics"), str) or not data["semantics"].strip():
        raise ValueError("public analytics semantics are missing")
    for name in ("months", "boroughs", "categories"):
        values = data.get(name)
        if (
            not isinstance(values, list)
            or not values
            or any(not isinstance(v, str) for v in values)
        ):
            raise ValueError("analytics dimensions must be nonempty string lists")
        if values != sorted(set(values)):
            raise ValueError("analytics dimensions must be sorted and unique")
    if any(not re.fullmatch(MONTH_PATTERN, month) for month in data["months"]):
        raise ValueError("analytics month dimension is invalid")
    if data["months"][0] < data["source_start"][:7] or data["months"][-1] > data["source_end"][:7]:
        raise ValueError("analytics months fall outside source coverage")
    facts = data.get("facts")
    if not isinstance(facts, list):
        raise ValueError("analytics facts are missing")
    fact_totals = defaultdict(int)
    seen = set()
    for row in facts:
        if not isinstance(row, list) or len(row) != 8 or any(not _integer(v) for v in row):
            raise ValueError("analytics facts require eight nonnegative integers")
        month, borough, category, requests, unresolved, *quality = row
        if (
            month >= len(data["months"])
            or borough >= len(data["boroughs"])
            or category >= len(data["categories"])
        ):
            raise ValueError("analytics fact dimension index is invalid")
        if unresolved > requests or any(value > requests for value in quality):
            raise ValueError("analytics fact quality/status count exceeds request count")
        key = (month, borough, category)
        if key in seen:
            raise ValueError("analytics facts have duplicate monthly dimension groups")
        seen.add(key)
        fact_totals[data["months"][month]] += requests
    daily = data.get("daily")
    if not isinstance(daily, list) or not daily:
        raise ValueError("analytics daily audit totals are missing")
    daily_totals = defaultdict(int)
    previous = None
    for row in daily:
        if not isinstance(row, list) or len(row) != 2 or not _integer(row[1]):
            raise ValueError("analytics daily rows are malformed")
        day = _day(row[0])
        if not first <= day <= last or previous is not None and day <= previous:
            raise ValueError("analytics daily dates are not sorted unique source dates")
        daily_totals[row[0][:7]] += row[1]
        previous = day
    if sum(fact_totals.values()) != data["source_records"] or dict(fact_totals) != dict(
        daily_totals
    ):
        raise ValueError("analytics monthly/daily totals do not match the full source audit")
    return data


@lru_cache(maxsize=2)
def _load_cached(path: str, modified_ns: int, size: int):
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return validate_artifact(json.load(handle))


def load_artifact():
    configured = os.getenv("PUBLIC_ANALYTICS_PATH", "artifacts/public-analytics.json.gz")
    try:
        if not configured.strip():
            raise ValueError("analytics artifact path is empty")
        path = Path(configured).resolve()
        stat = path.stat()
        return _load_cached(str(path), stat.st_mtime_ns, stat.st_size)
    except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
        raise HTTPException(503, "verified analytics are temporarily unavailable") from error


def query_filters(
    start_month: Annotated[str | None, Query(pattern=MONTH_PATTERN)] = None,
    end_month: Annotated[str | None, Query(pattern=MONTH_PATTERN)] = None,
    borough: Annotated[str | None, Query(max_length=256)] = None,
    category: Annotated[str | None, Query(max_length=512)] = None,
):
    return {
        "start_month": start_month,
        "end_month": end_month,
        "borough": borough,
        "category": category,
    }


def resolve_filters(data, selected):
    filters = {**selected}
    filters["start_month"] = selected["start_month"] or data["months"][0]
    filters["end_month"] = selected["end_month"] or data["months"][-1]
    if filters["start_month"] > filters["end_month"]:
        raise HTTPException(422, "start_month must not follow end_month")
    if filters["start_month"] < data["months"][0] or filters["end_month"] > data["months"][-1]:
        raise HTTPException(422, "requested month range falls outside the verified source coverage")
    for name, dimension in (("borough", "boroughs"), ("category", "categories")):
        if filters[name] is not None and filters[name] not in data[dimension]:
            raise HTTPException(404, f"unknown {name} in the verified source dimensions")
    return filters


def source_metadata(data):
    return {
        "start": data["source_start"],
        "end": data["source_end"],
        "records": data["source_records"],
        "excluded_latest_source_day": data["excluded_latest_source_day"],
        "semantics": data["semantics"],
        "is_live": False,
    }


def partial_day_notice(data):
    return (
        f"Analytics include the possibly partial source day {data['excluded_latest_source_day']}. "
        "That day is excluded from model training. Status counts describe the observed snapshot, "
        "not historical backlog transitions."
    )


def aggregate(data, filters):
    months = {
        month: [0, 0, 0, 0, 0]
        for month in data["months"]
        if filters["start_month"] <= month <= filters["end_month"]
    }
    borough_index = (
        data["boroughs"].index(filters["borough"]) if filters["borough"] is not None else None
    )
    category_index = (
        data["categories"].index(filters["category"]) if filters["category"] is not None else None
    )
    by_borough, by_category = defaultdict(lambda: [0, 0]), defaultdict(lambda: [0, 0])
    for mi, bi, ci, requests, unresolved, missing, nonclosed, invalid in data["facts"]:
        month = data["months"][mi]
        if month not in months or borough_index is not None and bi != borough_index:
            continue
        if category_index is not None and ci != category_index:
            continue
        for index, value in enumerate((requests, unresolved, missing, nonclosed, invalid)):
            months[month][index] += value
        for target, name in (
            (by_borough, data["boroughs"][bi]),
            (by_category, data["categories"][ci]),
        ):
            target[name][0] += requests
            target[name][1] += unresolved
    totals = [sum(values[index] for values in months.values()) for index in range(5)]

    def ranked(values):
        return [
            {"name": name, "requests": counts[0], "unresolved": counts[1]}
            for name, counts in sorted(values.items(), key=lambda item: (-item[1][0], item[0]))
            if counts[0] > 0
        ]

    result = {
        "schema_version": 1,
        "captured_at": data["captured_at"],
        "source": source_metadata(data),
        "filters": filters,
        "summary": {
            "requests": totals[0],
            "unresolved_now": totals[1],
            "closed": totals[0] - totals[1],
            "current_closed_share": (totals[0] - totals[1]) / totals[0] if totals[0] else 0.0,
            "category_count": sum(counts[0] > 0 for counts in by_category.values()),
            "active_months": sum(values[0] > 0 for values in months.values()),
        },
        "series": [
            {"month": month, "requests": values[0], "unresolved": values[1]}
            for month, values in months.items()
        ],
        "boroughs": ranked(by_borough),
        "categories": ranked(by_category),
        "quality": dict(zip(QUALITY, totals[2:], strict=True)),
        "source_partial_day_notice": partial_day_notice(data),
    }
    return result, months


@router.get("/metadata")
def metadata():
    data = load_artifact()
    filters = resolve_filters(
        data, dict.fromkeys(("start_month", "end_month", "borough", "category"))
    )
    result, _ = aggregate(data, filters)
    return {
        "schema_version": 1,
        "captured_at": data["captured_at"],
        "source": result["source"],
        "range": {
            "start_month": data["months"][0],
            "end_month": data["months"][-1],
            "start_date": data["source_start"],
            "end_date": data["source_end"],
        },
        "dimensions": {
            "months": data["months"],
            "boroughs": data["boroughs"],
            "categories": data["categories"],
            "years": sorted({month[:4] for month in data["months"]}),
        },
        "summary": result["summary"],
        "quality": result["quality"],
        "source_partial_day_notice": result["source_partial_day_notice"],
    }


@router.get("")
def analytics(filters: Annotated[dict, Depends(query_filters)]):
    data = load_artifact()
    result, _ = aggregate(data, resolve_filters(data, filters))
    return result


def csv_cell(value):
    text = str(value)
    stripped = text.lstrip(" \t\r\n")
    if text[:1] in ("=", "+", "-", "@", "\t", "\r", "\n") or stripped[:1] in ("=", "+", "-", "@"):
        return "'" + text
    return text


@router.get("/export")
def export(filters: Annotated[dict, Depends(query_filters)]):
    data = load_artifact()
    filters = resolve_filters(data, filters)
    _, months = aggregate(data, filters)
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(
        ["month", "borough", "category", "requests", "unresolved_now", "closed", *QUALITY]
    )
    for month, values in months.items():
        writer.writerow(
            [
                month,
                csv_cell(filters["borough"] if filters["borough"] is not None else "All boroughs"),
                csv_cell(
                    filters["category"] if filters["category"] is not None else "All categories"
                ),
                values[0],
                values[1],
                values[0] - values[1],
                *values[2:],
            ]
        )
    return Response(
        output.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="urbanlake-monthly-analytics.csv"',
            "X-Content-Type-Options": "nosniff",
        },
    )
