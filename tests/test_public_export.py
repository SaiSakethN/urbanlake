import gzip
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest


@pytest.fixture
def exporter():
    path = Path(__file__).resolve().parents[1] / "scripts/export_public_analytics.py"
    spec = importlib.util.spec_from_file_location("urbanlake_public_export_tests", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def audit():
    return {
        "status": "passed",
        "bronze_rows": 6,
        "silver_rows": 6,
        "scope_end_exclusive": "2026-11-01T02:06:29.000001",
        "excluded_latest_source_day": "2026-11-01",
    }


def fact(month, borough, category, requests):
    return {
        "month": month,
        "borough": borough,
        "complaint_type": category,
        "requests": requests,
        "unresolved_now": 1,
        "closed_missing_date": 0,
        "nonclosed_with_date": 0,
        "invalid_resolution_count": 0,
        "unique_key": "DO_NOT_EXPORT_SOURCE_KEY",
        "address": "DO_NOT_EXPORT_ADDRESS",
        "latitude": "DO_NOT_EXPORT_COORDINATES",
        "resolution_hour_quantiles": [999],
    }


def test_dictionary_encoded_facts_preserve_exact_counts_and_exclude_private_fields(exporter):
    rows = [fact("2026-11", "QUEENS", "Noise", 2), fact("2026-10", "BRONX", "Heat", 4)]
    daily = [{"day": "2026-11-01", "requests": 2}, {"day": "2026-10-31", "requests": 4}]
    snapshot = exporter.build_snapshot(
        rows, daily, audit(), captured_at="2026-11-04T12:00:00+00:00"
    )
    assert snapshot["months"] == ["2026-10", "2026-11"]
    assert snapshot["boroughs"] == ["BRONX", "QUEENS"]
    assert snapshot["categories"] == ["Heat", "Noise"]
    assert snapshot["facts"] == [[0, 0, 0, 4, 1, 0, 0, 0], [1, 1, 1, 2, 1, 0, 0, 0]]
    assert snapshot["daily"] == [["2026-10-31", 4], ["2026-11-01", 2]]
    assert snapshot["source_start"] == "2026-10-31"
    assert snapshot["source_end"] == snapshot["excluded_latest_source_day"] == "2026-11-01"
    assert snapshot["source_records"] == sum(row[3] for row in snapshot["facts"]) == 6
    serialized = json.dumps(snapshot)
    for forbidden in ("DO_NOT_EXPORT", "unique_key", "address", "latitude", "quantiles"):
        assert forbidden not in serialized
    assert "not CDC" in snapshot["semantics"]


@pytest.mark.parametrize("mismatch", ["facts", "daily", "audit"])
def test_inconsistent_totals_are_refused_before_publication(exporter, mismatch):
    rows = [fact("2026-11", "QUEENS", "Noise", 6)]
    daily = [{"day": "2026-11-01", "requests": 6}]
    report = audit()
    if mismatch == "facts":
        rows[0]["requests"] = 5
    elif mismatch == "daily":
        daily[0]["requests"] = 5
    else:
        report["status"] = "failed"
    with pytest.raises(ValueError):
        exporter.build_snapshot(rows, daily, report)


def test_duplicate_cells_and_bad_counts_are_refused(exporter):
    row = fact("2026-11", "QUEENS", "Noise", 3)
    with pytest.raises(ValueError, match="duplicate monthly"):
        exporter.build_snapshot([row, row], [{"day": "2026-11-01", "requests": 6}], audit())
    row["requests"] = 6.5
    with pytest.raises(ValueError, match="integer"):
        exporter.build_snapshot([row], [{"day": "2026-11-01", "requests": 6}], audit())


def test_gzip_bytes_and_checksum_are_deterministic_for_same_snapshot(exporter, tmp_path):
    snapshot = exporter.build_snapshot(
        [fact("2026-11", "QUEENS", "Noise", 6)],
        [{"day": "2026-11-01", "requests": 6}],
        audit(),
        captured_at="2026-11-04T12:00:00+00:00",
    )
    first, second = tmp_path / "first.json.gz", tmp_path / "second.json.gz"
    report = exporter.write_snapshot(first, snapshot)
    exporter.write_snapshot(second, snapshot)
    assert first.read_bytes() == second.read_bytes()
    assert json.loads(gzip.decompress(first.read_bytes())) == snapshot
    expected = hashlib.sha256(first.read_bytes()).hexdigest()
    assert report["sha256"] == expected
    assert first.with_name(first.name + ".sha256").read_text().strip() == expected
    assert report["source_records"] == 6
    assert not list(tmp_path.glob("*.part"))
