"""Bounded read-only size/freshness probe before full historical ingestion."""

import json
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path

from urbanlake.contracts import canonical
from urbanlake.source import Socrata

started = time.monotonic()
with Socrata() as source:
    summary = source.query(
        {
            "$select": (
                "count(*) AS rows,min(created_date) AS first_created,"
                "max(created_date) AS last_created"
            ),
            "$limit": 1,
        }
    )
    years = source.query(
        {
            "$select": "date_extract_y(created_date) AS year,count(*) AS rows",
            "$group": "date_extract_y(created_date)",
            "$order": "year",
            "$limit": 100,
        }
    )
    sample = source.query({"$order": "created_date DESC,unique_key DESC", "$limit": 100})
average = sum(len(canonical(row).encode()) for row in sample) / max(1, len(sample))
disk = shutil.disk_usage(Path.cwd())
report = {
    "probed_at": datetime.now(UTC).isoformat(),
    "summary": summary,
    "year_counts": years,
    "sample_rows": len(sample),
    "average_raw_row_bytes": round(average),
    "estimated_raw_json_gb": round(int(summary[0]["rows"]) * average / 1e9, 2),
    "disk_free_gb": round(disk.free / 1e9, 2),
    "probe_seconds": round(time.monotonic() - started, 2),
}
Path("artifacts").mkdir(exist_ok=True)
Path("artifacts/full-source-probe.json").write_text(json.dumps(report, indent=2))
print(json.dumps(report, indent=2))
