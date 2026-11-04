"""Exercise the packaged web app using real model and aggregate artifacts."""

import csv
import io
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient

workspace = Path.cwd()
release_info = json.loads((workspace / "artifacts/app-release-check.json").read_text())
release = Path(release_info["directory"])
sys.path.insert(0, str(release))
os.chdir(release)

from app import app  # noqa: E402

with TestClient(app) as client:
    page = client.get("/")
    page.raise_for_status()
    views = ("overview", "explore", "forecast", "quality", "architecture")
    assert all(f'id="view-{name}"' in page.text for name in views)
    metadata = client.get("/v1/analytics/metadata").json()
    assert metadata["source"]["records"] == 23008706
    assert len(metadata["dimensions"]["categories"]) == 276
    full = client.get("/v1/analytics").json()
    assert full["summary"]["requests"] == 23008706
    assert full["summary"]["closed"] == 22531441
    assert full["summary"]["unresolved_now"] == 477265
    assert sum(row["requests"] for row in full["series"]) == 23008706
    filters = {"start_month": "2026-01", "end_month": "2026-11", "borough": "QUEENS"}
    selected = client.get("/v1/analytics", params=filters).json()
    assert selected["summary"]["requests"] == 830304
    exported = client.get("/v1/analytics/export", params=filters)
    exported.raise_for_status()
    rows = list(csv.DictReader(io.StringIO(exported.text)))
    assert len(rows) == 11 and sum(int(row["requests"]) for row in rows) == 830304
    project = client.get("/v1/project").json()
    assert project["audit"]["status"] == "passed"
    assert project["audit"]["silver_rows"] == 23008706
    model = client.get("/v1/model").json()
    response = client.post(
        "/v1/predictions/demand", json={"target_date": model["supported_target_date"]}
    )
    response.raise_for_status()
    prediction = response.json()
    assert prediction["predicted_requests"] == 10922.37
    assert client.get("/ready").json()["cache"] == "disabled"
    assert client.get("/v1/analytics", params={"start_month": "2030-01"}).status_code == 422
    assert client.get("/progress").status_code == client.get("/docs").status_code == 200
    report = {
        "status": "passed",
        "verified_at": datetime.now(UTC).isoformat(),
        "environment": "local packaged app; FastAPI TestClient; actual aggregate/model assets",
        "views": list(views),
        "full_requests": full["summary"]["requests"],
        "category_count": len(metadata["dimensions"]["categories"]),
        "filtered_queens_2026": selected["summary"]["requests"],
        "csv_rows": len(rows),
        "quality": full["quality"],
        "prediction": prediction,
    }
    (workspace / "artifacts/app-release-verification.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    print(json.dumps(report, indent=2))
