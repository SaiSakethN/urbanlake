"""Resumable bounded-page archival backfill of NYC 311 source observations.

Each monthly state.json is a manifest. Consumers must use its ``files`` list,
never blindly glob pages: a crash may leave an uncommitted content-addressed page.
Source queries are observations of a mutable API, not a transactional snapshot.
"""

import argparse
import gzip
import hashlib
import json
import math
import os
import re
import shutil
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from urbanlake.contracts import Observation, SourceRequest
from urbanlake.source import Socrata

SCHEMA_VERSION = 1
ISO_LOCAL = re.compile(r"^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)?$")


def local_datetime(value: str) -> datetime:
    if not ISO_LOCAL.fullmatch(value):
        raise ValueError("bounds/cursors must be ISO dates or floating NYC datetimes")
    return datetime.fromisoformat(value)


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.part")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


@contextmanager
def run_lock(root: Path):
    """OS-released advisory lock: a killed process cannot strand a stale PID lock."""
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".backfill.lock").open("a+b") as handle:
        if handle.seek(0, os.SEEK_END) == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise RuntimeError("another backfill process owns this root") from error
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def create_plan(root: Path, start: str, end: str, run_id: str | None = None) -> dict:
    start_time, end_time = local_datetime(start), local_datetime(end)
    if start_time >= end_time:
        raise ValueError("start must precede exclusive end")
    if (end_time - start_time).days > 365 * 30:
        raise ValueError("refusing a source scope longer than 30 years")
    identifier = run_id or f"backfill-{uuid4().hex[:16]}"
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", identifier):
        raise ValueError("run-id must use 1..100 letters, digits, underscores or hyphens")
    partitions = []
    cursor = start_time
    while cursor < end_time:
        month_end = (
            cursor.replace(
                year=cursor.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
            )
            if cursor.month == 12
            else cursor.replace(
                month=cursor.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0
            )
        )
        boundary = min(month_end, end_time)
        partitions.append(
            {
                "partition_id": cursor.strftime("%Y-%m"),
                "start": cursor.isoformat(),
                "end": boundary.isoformat(),
            }
        )
        cursor = boundary
    plan = {
        "schema_version": SCHEMA_VERSION,
        "dataset": "erm2-nwe9",
        "run_id": identifier,
        "start": start_time.isoformat(),
        "end": end_time.isoformat(),
        "observed_at": datetime.now(UTC).isoformat(),
        "partitions": partitions,
        "semantics": "mutable-source observations; not CDC or a transactional snapshot",
    }
    with run_lock(root):
        destination = root / "plan.json"
        if destination.exists():
            existing = json.loads(destination.read_text(encoding="utf-8"))
            if existing["start"] != plan["start"] or existing["end"] != plan["end"]:
                raise ValueError("existing plan has different bounds; choose a new root")
            if run_id and existing["run_id"] != run_id:
                raise ValueError("existing plan has another run-id; choose a new root")
            return existing
        atomic_json(destination, plan)
        atomic_json(root / "status.json", summary(root, plan))
    return plan


def summary(root: Path, plan: dict | None = None) -> dict:
    plan = plan or json.loads((root / "plan.json").read_text(encoding="utf-8"))
    states = []
    for partition in plan["partitions"]:
        path = root / "partitions" / partition["partition_id"] / "state.json"
        if path.exists():
            states.append(json.loads(path.read_text(encoding="utf-8")))
    completed = sum(bool(state["complete"]) for state in states)
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": plan["run_id"],
        "start": plan["start"],
        "end": plan["end"],
        "total_partitions": len(plan["partitions"]),
        "completed_partitions": completed,
        "complete": completed == len(plan["partitions"]),
        "records": sum(state["records"] for state in states),
        "pages": sum(len(state["pages"]) for state in states),
        "compressed_bytes": sum(state["bytes"] for state in states),
        "failed_partitions": [
            state["partition_id"]
            for state in states
            if state["status"] in ("failed", "needs_reconciliation")
        ],
        "updated_at": datetime.now(UTC).isoformat(),
    }


def check_disk(root: Path, min_free_gb: float, extra_bytes: int = 64 * 1024**2) -> None:
    if not math.isfinite(min_free_gb) or min_free_gb < 0:
        raise ValueError("minimum free disk must be nonnegative")
    free = shutil.disk_usage(root).free
    if free < min_free_gb * 1024**3 + extra_bytes:
        raise RuntimeError(f"disk safeguard stopped ingestion: {free / 1024**3:.2f} GiB free")


def _base_where(partition: dict) -> str:
    start = local_datetime(partition["start"]).isoformat()
    end = local_datetime(partition["end"]).isoformat()
    return f"created_date >= '{start}' AND created_date < '{end}'"


def _query_where(partition: dict, cursor: list[str] | None) -> str:
    where = _base_where(partition)
    if cursor:
        stamp, key = cursor
        local_datetime(stamp)
        if not re.fullmatch(r"\d+", key):
            raise ValueError("invalid source key in checkpoint")
        where += (
            f" AND (created_date > '{stamp}' OR "
            f"(created_date = '{stamp}' AND unique_key > '{key}'))"
        )
    return where


def _source_count(source, partition: dict) -> int:
    rows = source.query({"$select": "count(*) AS rows", "$where": _base_where(partition)})
    if len(rows) != 1 or int(rows[0]["rows"]) < 0:
        raise ValueError("invalid source count response")
    return int(rows[0]["rows"])


def validate_page(rows: list[dict], partition: dict, cursor: list[str] | None) -> list[str]:
    previous = (local_datetime(cursor[0]), cursor[1]) if cursor else None
    start, end = local_datetime(partition["start"]), local_datetime(partition["end"])
    last = None
    for row in rows:
        parsed = SourceRequest.model_validate(row)
        # Validate exact raw timestamp before using it in a future SoQL predicate.
        stamp = row["created_date"]
        local_datetime(stamp)
        current = (parsed.created_date, parsed.unique_key)
        if not start <= parsed.created_date < end:
            raise ValueError("source returned a row outside the planned partition")
        if previous is not None and current <= previous:
            raise ValueError(
                "source cursor is not strictly increasing (duplicate/out-of-order row)"
            )
        previous = current
        last = [stamp, parsed.unique_key]
    if last is None:
        raise ValueError("cannot validate an empty page")
    return last


def write_page(root: Path, state: dict, rows: list[dict]) -> dict:
    observed_at = datetime.fromisoformat(state["observed_at"])
    payload = b"".join(
        (
            Observation.wrap(
                row, kind="request", run_id=state["run_id"], observed_at=observed_at
            ).model_dump_json()
            + "\n"
        ).encode("utf-8")
        for row in rows
    )
    compressed = gzip.compress(payload, compresslevel=3, mtime=0)
    checksum = hashlib.sha256(compressed).hexdigest()
    number = len(state["pages"])
    relative = (
        Path("partitions") / state["partition_id"] / f"page-{number:06d}-{checksum[:16]}.jsonl.gz"
    )
    destination = root / relative
    if destination.exists():
        if hashlib.sha256(destination.read_bytes()).hexdigest() != checksum:
            raise ValueError("existing content-addressed page has an invalid checksum")
    else:
        temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.part")
        with temporary.open("wb") as handle:
            handle.write(compressed)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(destination)
    return {
        "path": relative.as_posix(),
        "records": len(rows),
        "sha256": checksum,
        "bytes": len(compressed),
        "last_cursor": [rows[-1]["created_date"], rows[-1]["unique_key"]],
    }


class PageBudget:
    def __init__(self, maximum: int | None):
        self.maximum = maximum
        self.claimed = 0
        self.lock = threading.Lock()

    def claim(self) -> bool:
        with self.lock:
            if self.maximum is not None and self.claimed >= self.maximum:
                return False
            self.claimed += 1
            return True

    def exhausted(self) -> bool:
        with self.lock:
            return self.maximum is not None and self.claimed >= self.maximum


def download_partition(
    root: Path,
    plan: dict,
    partition: dict,
    *,
    page_size: int,
    min_free_gb: float,
    budget: PageBudget,
    stop: threading.Event,
    source_factory: Callable = Socrata,
) -> dict:
    if stop.is_set() or budget.exhausted():
        return {"status": "paused", "complete": False}
    directory = root / "partitions" / partition["partition_id"]
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "state.json"
    if path.exists():
        state = json.loads(path.read_text(encoding="utf-8"))
        if state["complete"]:
            return state
    else:
        state = {
            "schema_version": SCHEMA_VERSION,
            **partition,
            "run_id": f"{plan['run_id']}-{partition['partition_id']}",
            "observed_at": datetime.now(UTC).isoformat(),
            "complete": False,
            "status": "pending",
            "records": 0,
            "bytes": 0,
            "pages": [],
            "files": [],
            "cursor": None,
        }
        atomic_json(path, state)
    try:
        # Verify the last committed page on resume before trusting its cursor.
        if state["pages"]:
            page = state["pages"][-1]
            candidate = root / page["path"]
            if (
                not candidate.is_file()
                or hashlib.sha256(candidate.read_bytes()).hexdigest() != page["sha256"]
            ):
                raise ValueError("last committed backfill page is missing or corrupted")
        with source_factory() as source:
            if "source_count_initial" not in state:
                state["source_count_initial"] = _source_count(source, partition)
            state["status"] = "running"
            state.pop("error", None)
            atomic_json(path, state)
            while not stop.is_set():
                check_disk(root, min_free_gb)
                if not budget.claim():
                    state["status"] = "paused"
                    break
                rows = source.query(
                    {
                        "$where": _query_where(partition, state["cursor"]),
                        "$order": "created_date ASC, unique_key ASC",
                        "$limit": page_size,
                    }
                )
                if len(rows) > page_size:
                    raise ValueError("source returned more records than the bounded query")
                if not rows:
                    state["source_count_final"] = _source_count(source, partition)
                    if state["records"] != state["source_count_initial"] or (
                        state["records"] != state["source_count_final"]
                    ):
                        state["status"] = "needs_reconciliation"
                        atomic_json(path, state)
                        raise RuntimeError(
                            "source count changed or records are missing; partition is incomplete"
                        )
                    state["complete"] = True
                    state["status"] = "complete"
                    break
                cursor = validate_page(rows, partition, state["cursor"])
                page = write_page(root, state, rows)
                page["fetched_at"] = datetime.now(UTC).isoformat()
                check_disk(root, min_free_gb, extra_bytes=0)
                state["pages"].append(page)
                state["files"].append(page["path"])
                state["records"] += page["records"]
                state["bytes"] += page["bytes"]
                state["cursor"] = cursor
                state["updated_at"] = datetime.now(UTC).isoformat()
                atomic_json(path, state)
                print(
                    json.dumps(
                        {
                            "partition": state["partition_id"],
                            "records": state["records"],
                            "pages": len(state["pages"]),
                            "complete": False,
                        }
                    ),
                    flush=True,
                )
            if stop.is_set() and not state["complete"]:
                state["status"] = "paused"
            atomic_json(path, state)
            return state
    except Exception as error:
        if state["status"] != "needs_reconciliation":
            state["status"] = "failed"
        state["error"] = f"{type(error).__name__}: {error}"
        atomic_json(path, state)
        stop.set()
        raise


def run(
    root: Path,
    *,
    workers: int = 1,
    page_size: int = 5000,
    min_free_gb: float = 30,
    max_pages: int | None = None,
    partition: str | None = None,
    source_factory: Callable = Socrata,
) -> dict:
    if workers not in (1, 2) or not 1 <= page_size <= 5000:
        raise ValueError("workers must be 1 or 2; page-size must be 1..5000")
    if max_pages is not None and max_pages < 1:
        raise ValueError("max-pages must be positive")
    if not math.isfinite(min_free_gb) or min_free_gb < 0:
        raise ValueError("min-free-gb must be nonnegative")
    with run_lock(root):
        plan = json.loads((root / "plan.json").read_text(encoding="utf-8"))
        partitions = [
            item
            for item in plan["partitions"]
            if partition is None or item["partition_id"] == partition
        ]
        if not partitions:
            raise ValueError("partition is not included in this plan")
        budget, stop = PageBudget(max_pages), threading.Event()
        errors = []
        executor = ThreadPoolExecutor(max_workers=workers)
        futures = []
        try:
            futures = [
                executor.submit(
                    download_partition,
                    root,
                    plan,
                    partition,
                    page_size=page_size,
                    min_free_gb=min_free_gb,
                    budget=budget,
                    stop=stop,
                    source_factory=source_factory,
                )
                for partition in partitions
            ]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as error:
                    errors.append(str(error))
                atomic_json(root / "status.json", summary(root, plan))
        except KeyboardInterrupt:
            stop.set()
            for future in futures:
                future.cancel()
            raise
        finally:
            executor.shutdown(wait=True, cancel_futures=stop.is_set())
            result = summary(root, plan)
            result["errors"] = errors
            atomic_json(root / "status.json", result)
        if errors:
            raise RuntimeError(f"backfill incomplete: {errors}")
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    command = sub.add_parser("plan")
    command.add_argument("--start", required=True)
    command.add_argument("--end", required=True, help="exclusive floating NYC datetime")
    command.add_argument("--run-id")
    command.add_argument("--root", type=Path, default=Path("data/full/backfill"))
    command = sub.add_parser("run")
    command.add_argument("--root", type=Path, default=Path("data/full/backfill"))
    command.add_argument("--workers", type=int, default=1)
    command.add_argument("--page-size", type=int, default=5000)
    command.add_argument("--min-free-gb", type=float, default=30)
    command.add_argument("--max-pages", type=int, help="optional finite pilot budget, resumable")
    command.add_argument("--partition", help="download just one planned YYYY-MM partition")
    command = sub.add_parser("status")
    command.add_argument("--root", type=Path, default=Path("data/full/backfill"))
    args = parser.parse_args()
    if args.command == "plan":
        result = create_plan(args.root, args.start, args.end, args.run_id)
    elif args.command == "run":
        result = run(
            args.root,
            workers=args.workers,
            page_size=args.page_size,
            min_free_gb=args.min_free_gb,
            max_pages=args.max_pages,
            partition=args.partition,
        )
    else:
        result = summary(args.root)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
