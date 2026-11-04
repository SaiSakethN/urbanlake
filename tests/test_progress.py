import json

import pytest
from fastapi.testclient import TestClient

from urbanlake.api import app
from urbanlake.progress import snapshot


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def setup_run(root):
    source = root / "data/full/backfill"
    write(
        source / "plan.json",
        {
            "partitions": [
                {"partition_id": "2020-01"},
                {"partition_id": "2020-02"},
            ]
        },
    )
    write(root / "artifacts/full-source-probe.json", {"summary": [{"rows": "100"}]})
    write(root / "artifacts/full-project-status.json", {"status": "running", "complete": False})
    write(
        source / "pipeline.json",
        {
            "status": "running",
            "current_partition": "2020-02",
            "current_stage": "consume",
            "partitions": {"2020-01": {"complete": True}},
        },
    )
    write(source / "partitions/2020-01/state.json", {"records": 60, "complete": True})
    write(source / "partitions/2020-02/state.json", {"records": 10, "complete": False})
    return source


def test_live_counts_change_only_when_saved_state_changes(tmp_path):
    source = setup_run(tmp_path)
    before = snapshot(tmp_path)
    assert before["downloaded_records"] == 70 and before["processed_records"] == 60
    assert before["overall_percent"] == 57
    write(source / "partitions/2020-02/state.json", {"records": 25, "complete": False})
    after = snapshot(tmp_path)
    assert after["downloaded_records"] == 85 and after["processed_records"] == 60
    assert after["completed_months"] == 1


def test_full_download_does_not_claim_completed_project(tmp_path):
    source = setup_run(tmp_path)
    write(source / "partitions/2020-02/state.json", {"records": 40, "complete": True})
    write(
        source / "pipeline.json",
        {
            "partitions": {"2020-01": {"complete": True}, "2020-02": {"complete": True}},
            "verification": {"complete": True},
        },
    )
    write(
        tmp_path / "artifacts/full-project-status.json",
        {
            "status": "running",
            "stage": "training",
            "complete": False,
        },
    )
    state = snapshot(tmp_path)
    assert state["download_percent"] == 100 and state["processing_percent"] == 100
    assert state["overall_percent"] == 94
    write(
        tmp_path / "artifacts/full-project-status.json",
        {
            "status": "complete",
            "stage": "complete",
            "complete": True,
        },
    )
    assert snapshot(tmp_path)["overall_percent"] == 100


def test_progress_api_reads_real_files_without_cache(monkeypatch, tmp_path):
    setup_run(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MODEL_PATH", str(tmp_path / "missing.joblib"))
    with TestClient(app) as client:
        response = client.get("/v1/progress")
        assert response.status_code == 200
        assert response.headers["cache-control"] == "no-store"
        assert response.json()["downloaded_records"] == 70


def test_failure_is_visible_without_exposing_local_paths(tmp_path):
    setup_run(tmp_path)
    write(
        tmp_path / "artifacts/full-project-status.json",
        {
            "status": "failed",
            "error": "private local path or process error",
        },
    )
    state = snapshot(tmp_path)
    assert state["status"] == "failed" and state["overall_percent"] < 100
    assert "private local" not in json.dumps(state)


def completed_capture(root):
    source = setup_run(root)
    write(source / "partitions/2020-02/state.json", {"records": 40, "complete": True})
    write(
        source / "pipeline.json",
        {
            "partitions": {"2020-01": {"complete": True}, "2020-02": {"complete": True}},
            "verification": {"complete": True},
        },
    )
    write(
        root / "artifacts/full-project-status.json",
        {
            "status": "complete",
            "stage": "complete",
            "complete": True,
        },
    )
    return snapshot(root)


def test_hosted_capture_is_frozen_and_does_not_require_local_pipeline(monkeypatch, tmp_path):
    captured = completed_capture(tmp_path)
    file = tmp_path / "verified-progress.json"
    write(file, captured)
    monkeypatch.setenv("PROGRESS_SNAPSHOT_PATH", str(file))
    first = snapshot(tmp_path / "no-runtime-state")
    second = snapshot(tmp_path / "no-runtime-state")
    assert first == second
    assert first["snapshot_at"] == captured["snapshot_at"]
    assert first["captured_at"] == captured["snapshot_at"]
    assert first["mode"] == "verified_snapshot"


def test_completed_local_run_elapsed_is_capped_at_recorded_finish(tmp_path):
    completed_capture(tmp_path)
    pipeline_path = tmp_path / "data/full/backfill/pipeline.json"
    pipeline = json.loads(pipeline_path.read_text())
    pipeline["started_at"] = "2026-01-01T00:00:00+00:00"
    write(pipeline_path, pipeline)
    project_path = tmp_path / "artifacts/full-project-status.json"
    project = json.loads(project_path.read_text())
    project["finished_at"] = "2026-01-01T00:10:00+00:00"
    write(project_path, project)
    assert snapshot(tmp_path)["elapsed_seconds"] == 600


@pytest.mark.parametrize(
    "change",
    [
        {"status": "running"},
        {"overall_percent": 99},
        {"processed_records": 99},
        {"completed_months": 1},
        {"months": []},
        {"phases": []},
        {"snapshot_at": "2026-11-04"},
    ],
)
def test_inconsistent_hosted_capture_is_rejected(monkeypatch, tmp_path, change):
    captured = completed_capture(tmp_path)
    write(tmp_path / "verified.json", {**captured, **change})
    monkeypatch.setenv("PROGRESS_SNAPSHOT_PATH", str(tmp_path / "verified.json"))
    with pytest.raises(ValueError):
        snapshot(tmp_path)


@pytest.mark.parametrize("content", [None, "invalid JSON", "[]"])
def test_missing_or_malformed_hosted_capture_returns_503_without_local_fallback(
    monkeypatch, tmp_path, content
):
    completed_capture(tmp_path)
    file = tmp_path / "hosted.json"
    if content is not None:
        file.write_text(content)
    monkeypatch.setenv("PROGRESS_SNAPSHOT_PATH", str(file))
    monkeypatch.setenv("REDIS_ENABLED", "false")
    monkeypatch.setenv("MODEL_PATH", str(tmp_path / "missing-model.joblib"))
    with TestClient(app) as client:
        response = client.get("/v1/progress")
        assert response.status_code == 503
        assert "error" in response.json()
