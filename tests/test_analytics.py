import csv
import gzip
import io
import json
import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from urbanlake.analytics import _load_cached, router


@pytest.fixture
def analytics_data():
    return {
        "schema_version": 1,
        "captured_at": "2026-11-04T08:00:00+00:00",
        "source_start": "2020-01-01",
        "source_end": "2020-03-01",
        "source_records": 24,
        "excluded_latest_source_day": "2020-03-01",
        "months": ["2020-01", "2020-02", "2020-03"],
        "boroughs": ["BRONX", "QUEENS"],
        "categories": ["=Unsafe category", "Noise", "Water"],
        "facts": [
            [0, 0, 1, 10, 2, 1, 1, 0],
            [0, 1, 2, 4, 1, 0, 1, 0],
            [1, 1, 1, 7, 3, 1, 0, 1],
            [2, 0, 0, 3, 1, 0, 0, 0],
        ],
        "daily": [["2020-01-01", 14], ["2020-02-01", 7], ["2020-03-01", 3]],
        "semantics": "Observed current source snapshot; not CDC or historical backlog.",
    }


@pytest.fixture
def client(monkeypatch, tmp_path, analytics_data):
    path = tmp_path / "public.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump(analytics_data, handle)
    monkeypatch.setenv("PUBLIC_ANALYTICS_PATH", str(path))
    _load_cached.cache_clear()
    application = FastAPI()
    application.include_router(router)
    with TestClient(application) as value:
        yield value


def test_metadata_exposes_complete_audit_and_dimensions(client):
    response = client.get("/v1/analytics/metadata")
    assert response.status_code == 200
    data = response.json()
    assert data["source"]["records"] == 24 and data["source"]["is_live"] is False
    assert data["summary"] == {
        "requests": 24,
        "unresolved_now": 7,
        "closed": 17,
        "current_closed_share": 17 / 24,
        "category_count": 3,
        "active_months": 3,
    }
    assert data["dimensions"]["years"] == ["2020"]
    assert "possibly partial" in data["source_partial_day_notice"]


def test_filters_share_one_consistent_aggregation(client):
    data = client.get(
        "/v1/analytics",
        params={
            "start_month": "2020-01",
            "end_month": "2020-02",
            "borough": "QUEENS",
        },
    ).json()
    assert data["summary"]["requests"] == 11
    assert data["source"]["records"] == 24
    assert data["summary"]["unresolved_now"] == 4
    assert data["quality"] == {
        "closed_missing_date": 1,
        "nonclosed_with_date": 1,
        "invalid_resolution_count": 1,
    }
    assert data["series"] == [
        {"month": "2020-01", "requests": 4, "unresolved": 1},
        {"month": "2020-02", "requests": 7, "unresolved": 3},
    ]
    assert data["categories"][0] == {"name": "Noise", "requests": 7, "unresolved": 3}
    assert data["boroughs"] == [{"name": "QUEENS", "requests": 11, "unresolved": 4}]


def test_empty_combination_returns_real_zeroes(client):
    data = client.get("/v1/analytics", params={"borough": "BRONX", "category": "Water"}).json()
    assert data["summary"]["requests"] == 0
    assert data["summary"]["current_closed_share"] == 0
    assert data["categories"] == []
    assert all(row["requests"] == 0 for row in data["series"])


@pytest.mark.parametrize(
    "params,status",
    [
        ({"start_month": "2020-13"}, 422),
        ({"start_month": "2020-03", "end_month": "2020-01"}, 422),
        ({"start_month": "2019-12"}, 422),
        ({"borough": "unknown"}, 404),
        ({"category": "unknown"}, 404),
    ],
)
def test_invalid_filters_are_explicit(client, params, status):
    assert client.get("/v1/analytics", params=params).status_code == status


def test_csv_is_monthly_aggregate_and_escapes_spreadsheet_formulas(client):
    response = client.get("/v1/analytics/export", params={"category": "=Unsafe category"})
    assert response.status_code == 200
    assert "attachment" in response.headers["content-disposition"]
    rows = list(csv.DictReader(io.StringIO(response.text)))
    assert len(rows) == 3
    assert rows[2]["category"] == "'=Unsafe category"
    assert sum(int(row["requests"]) for row in rows) == 3


def test_missing_artifact_returns_503(monkeypatch, client, tmp_path):
    monkeypatch.setenv("PUBLIC_ANALYTICS_PATH", str(tmp_path / "missing.gz"))
    assert client.get("/v1/analytics/metadata").status_code == 503


def test_inconsistent_artifact_never_returns_partial_counts(
    monkeypatch, client, tmp_path, analytics_data
):
    path = tmp_path / "corrupt-counts.json.gz"
    analytics_data["source_records"] = 25
    with gzip.open(path, "wt") as handle:
        json.dump(analytics_data, handle)
    monkeypatch.setenv("PUBLIC_ANALYTICS_PATH", str(path))
    assert client.get("/v1/analytics").status_code == 503


def test_artifact_is_cached_until_file_changes(monkeypatch, client, tmp_path, analytics_data):
    path = tmp_path / "cached.json.gz"
    with gzip.open(path, "wt") as handle:
        json.dump(analytics_data, handle)
    monkeypatch.setenv("PUBLIC_ANALYTICS_PATH", str(path))
    assert client.get("/v1/analytics/metadata").status_code == 200
    first = _load_cached.cache_info()
    assert client.get("/v1/analytics/metadata").status_code == 200
    assert _load_cached.cache_info().hits > first.hits
    before = path.stat()
    analytics_data["captured_at"] = "2026-11-04T08:01:00+00:00"
    with gzip.open(path, "wt") as handle:
        json.dump(analytics_data, handle)
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns + 1000000))
    updated = client.get("/v1/analytics/metadata")
    assert updated.status_code == 200
    assert updated.json()["captured_at"] == "2026-11-04T08:01:00+00:00"


def test_malformed_gzip_returns_503(monkeypatch, client, tmp_path):
    path = tmp_path / "malformed.gz"
    path.write_bytes(b"not a gzip archive")
    monkeypatch.setenv("PUBLIC_ANALYTICS_PATH", str(path))
    assert client.get("/v1/analytics").status_code == 503
