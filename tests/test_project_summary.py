import json

import pytest

from urbanlake import project_summary


def audit():
    return {
        "status": "passed",
        "source_months": 1,
        "archived_observations": 3,
        "bronze_rows": 3,
        "distinct_observation_ids": 3,
        "distinct_source_keys": 3,
        "silver_rows": 3,
        "silver_distinct_source_keys": 3,
        "gold_demand_requests": 3,
        "gold_analytics_requests": 3,
        "invalid_requests": 0,
        "source_reference_count_matches": True,
        "internal_secret": "never publish this value",
    }


def test_project_summary_reconciles_and_excludes_unlisted_fields(tmp_path):
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    (artifacts / "full-verification.json").write_text(json.dumps(audit()))
    (artifacts / "full-model.metrics.json").write_text(
        json.dumps({"test_rows": 497, "test_selected_mae": 788.4, "private_path": "secret"})
    )
    result = project_summary.build(tmp_path, captured_at="2026-11-04T08:00:00+00:00")
    assert result["captured_at"] == "2026-11-04T08:00:00+00:00"
    assert result["audit"]["silver_rows"] == 3
    assert result["model_training"] == {"test_rows": 497, "test_selected_mae": 788.4}
    assert "secret" not in json.dumps(result)


def test_project_summary_rejects_mismatched_audit(tmp_path):
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    bad = audit()
    bad["gold_analytics_requests"] = 2
    (artifacts / "full-verification.json").write_text(json.dumps(bad))
    with pytest.raises(ValueError, match="reconcile"):
        project_summary.build(tmp_path)


def test_hosted_project_evidence_does_not_fall_back_to_local_files(monkeypatch, tmp_path):
    monkeypatch.setenv("PROJECT_OVERVIEW_PATH", str(tmp_path / "missing.json"))
    with pytest.raises(FileNotFoundError):
        project_summary.overview(tmp_path)
