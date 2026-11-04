"""Bounded SODA 2.1 queries with deterministic keyset pagination."""

import os
import time
from collections.abc import Iterator
from datetime import date, datetime, timedelta

import httpx

from urbanlake.contracts import SourceRequest

ENDPOINT = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"


class WindowTooLarge(ValueError):
    pass


class IncompleteWindow(RuntimeError):
    pass


def validate_window(start: date, end: date, max_days: int = 366) -> None:
    if not 0 < (end - start).days <= max_days:
        raise WindowTooLarge(f"window must be 1..{max_days} days, end exclusive")


class Socrata:
    def __init__(self, client: httpx.Client | None = None, sleep=time.sleep):
        headers = {"User-Agent": "UrbanLake/0.1 (bounded portfolio ingestion)"}
        if token := os.getenv("SOCRATA_APP_TOKEN"):
            headers["X-App-Token"] = token
        self.client = client or httpx.Client(timeout=60, headers=headers)
        self.sleep = sleep

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.client.close()

    def query(self, params: dict) -> list[dict]:
        for attempt in range(5):
            try:
                response = self.client.get(ENDPOINT, params=params)
                if response.status_code == 429 or response.status_code >= 500:
                    if attempt == 4:
                        response.raise_for_status()
                    delay = response.headers.get("Retry-After", "")
                    self.sleep(min(float(delay) if delay.isdigit() else 2**attempt, 60))
                    continue
                response.raise_for_status()
                rows = response.json()
                if not isinstance(rows, list) or any(not isinstance(r, dict) for r in rows):
                    raise ValueError("Socrata response must be a list of objects")
                return rows
            except httpx.TransportError:
                if attempt == 4:
                    raise
                self.sleep(2**attempt)
        raise RuntimeError("retry budget exhausted")

    @staticmethod
    def where(start: date, end: date) -> str:
        validate_window(start, end)
        return f"created_date >= '{start}T00:00:00' AND created_date < '{end}T00:00:00'"

    def requests(
        self, start: date, end: date, *, max_records: int = 10000, page_size: int = 1000
    ) -> Iterator[dict]:
        if not 1 <= page_size <= 5000 or not 1 <= max_records <= 100000:
            raise ValueError("page_size 1..5000, max_records 1..100000")
        base = self.where(start, end)
        cursor = None
        emitted = 0
        while True:
            where = base
            if cursor:
                created, key = cursor
                where += (
                    f" AND (created_date > '{created}' OR "
                    f"(created_date = '{created}' AND unique_key > '{key}'))"
                )
            # Fetch one extra at the cap to distinguish complete from truncated windows.
            rows = self.query(
                {
                    "$where": where,
                    "$order": "created_date ASC, unique_key ASC",
                    "$limit": min(page_size, max_records - emitted + 1),
                }
            )
            if not rows:
                return
            for row in rows:
                parsed = SourceRequest.model_validate(row)
                if (
                    not datetime.combine(start, datetime.min.time())
                    <= parsed.created_date
                    < (datetime.combine(end, datetime.min.time()))
                ):
                    raise ValueError("source row outside requested window")
                if emitted == max_records:
                    raise IncompleteWindow("record cap reached; split window and retry")
                yield row
                emitted += 1
            last = rows[-1]
            # Validate before interpolating server values into a subsequent SoQL expression.
            stamp = SourceRequest.model_validate(last).created_date.isoformat(
                timespec="milliseconds"
            )
            next_cursor = (stamp, last["unique_key"])
            if next_cursor == cursor:
                raise ValueError("source cursor did not advance")
            cursor = next_cursor

    def daily_demand(self, start: date, end: date) -> list[dict]:
        rows = self.query(
            {
                "$select": "date_trunc_ymd(created_date) AS day, count(*) AS requests",
                "$where": self.where(start, end),
                "$group": "date_trunc_ymd(created_date)",
                "$order": "day ASC",
                "$limit": 367,
            }
        )
        expected = {str(start + timedelta(days=n)) for n in range((end - start).days)}
        actual = [row["day"][:10] for row in rows]
        if len(actual) != len(set(actual)) or set(actual) != expected:
            raise IncompleteWindow("daily aggregate contains missing/duplicate/out-of-window dates")
        for row in rows:
            if int(row["requests"]) < 0:
                raise ValueError("negative request count")
        latest = self.query(
            {
                "$select": "created_date",
                "$order": "created_date DESC",
                "$limit": 1,
            }
        )
        if not latest or date.fromisoformat(latest[0]["created_date"][:10]) < end:
            raise IncompleteWindow("exclude the latest source day: it may be partially loaded")
        return rows
