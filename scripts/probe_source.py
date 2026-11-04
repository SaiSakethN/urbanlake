"""Read-only bounded freshness probe; never prints addresses or identifiers."""

import json
from datetime import date

from urbanlake.source import Socrata

with Socrata() as source:
    latest = source.query({"$select": "created_date", "$order": "created_date DESC", "$limit": 1})
    rows = source.query(
        {
            "$select": "date_trunc_ymd(created_date) AS day, count(*) AS requests",
            "$where": source.where(date(2026, 7, 6), date(2026, 11, 3)),
            "$group": "date_trunc_ymd(created_date)",
            "$order": "day ASC",
            "$limit": 367,
        }
    )
    print(
        json.dumps(
            {
                "latest_source_date": latest,
                "aggregate_days": len(rows),
                "first": rows[:2],
                "last": rows[-2:],
            },
            indent=2,
        )
    )
