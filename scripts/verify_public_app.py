"""Verify the actual public application, analytics filters, CSV and inference."""

import argparse
import csv
import io
import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--url", default="https://urbanlake-three.vercel.app")
args = parser.parse_args()
base = args.url.rstrip("/")
if urlparse(base).scheme != "https":
    raise ValueError("verify a public HTTPS deployment")

with httpx.Client(timeout=60, follow_redirects=True) as client:
    page = client.get(base + "/")
    page.raise_for_status()
    if urlparse(str(page.url)).netloc != urlparse(base).netloc:
        raise ValueError("public app redirects to another host or authentication page")
    views = ("overview", "explore", "forecast", "quality", "architecture")
    assert all(f'id="view-{name}"' in page.text for name in views)
    assert page.url.path == "/", "the root must serve the app, not redirect to progress"

    def get(path, params=None):
        response = client.get(base + path, params=params)
        response.raise_for_status()
        return response

    metadata = get("/v1/analytics/metadata").json()
    analytics = get("/v1/analytics").json()
    assert metadata["source"]["records"] == analytics["summary"]["requests"] == 23008706
    assert len(metadata["dimensions"]["categories"]) == 276
    assert len(metadata["dimensions"]["boroughs"]) == 7
    assert sum(row["requests"] for row in analytics["series"]) == 23008706
    assert len(analytics["series"]) == 83
    filters = {"start_month": "2026-01", "end_month": "2026-11", "borough": "QUEENS"}
    selected = get("/v1/analytics", filters).json()
    assert selected["summary"]["requests"] == 830304
    csv_response = get("/v1/analytics/export", filters)
    csv_rows = list(csv.DictReader(io.StringIO(csv_response.text)))
    assert len(csv_rows) == 11
    assert sum(int(row["requests"]) for row in csv_rows) == 830304
    category = metadata["dimensions"]["categories"][0]
    category_result = get("/v1/analytics", {"category": category}).json()
    assert category_result["summary"]["category_count"] == 1
    assert all(row["name"] == category for row in category_result["categories"])
    project = get("/v1/project").json()
    assert project["audit"]["status"] == "passed"
    assert project["audit"]["silver_rows"] == 23008706
    assert project["mode"] == "hosted_app"
    assert get("/ready").json()["cache"] == "disabled"
    model = get("/v1/model").json()
    assert model["model_version"] == "LOCAL_MODEL_VERSION"
    response = client.post(
        base + "/v1/predictions/demand",
        json={"schema_version": 1, "target_date": model["supported_target_date"]},
    )
    response.raise_for_status()
    prediction = response.json()
    assert prediction["predicted_requests"] == 10922.37
    assert client.get(base + "/v1/analytics", params={"borough": "invalid"}).status_code == 404
    assert get("/progress").status_code == get("/docs").status_code == 200
    result = {
        "status": "passed",
        "verified_at": datetime.now(UTC).isoformat(),
        "url": base,
        "views": list(views),
        "requests": analytics["summary"]["requests"],
        "source_months": len(analytics["series"]),
        "categories": len(metadata["dimensions"]["categories"]),
        "filtered_queens_2026": selected["summary"]["requests"],
        "csv_rows": len(csv_rows),
        "quality": analytics["quality"],
        "prediction": prediction,
        "project_capture": project["captured_at"],
        "visual_browser_check": "not performed; browser access host unavailable",
    }
    Path("artifacts/public-application-verification.json").write_text(
        json.dumps(result, indent=2), encoding="utf-8"
    )
    print(json.dumps(result, indent=2))
