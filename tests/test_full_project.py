import hashlib
import json

import httpx
import pytest

from urbanlake import full_project


@pytest.mark.parametrize("pipeline,audit", [(False, "passed"), (True, "failed")])
def test_incomplete_data_cannot_promote_a_full_model(monkeypatch, tmp_path, pipeline, audit):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["full_project", "--skip-pipeline"])
    root = tmp_path / "data/full/backfill"
    root.mkdir(parents=True)
    (root / "pipeline.json").write_text(json.dumps({"complete": pipeline}))
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    (artifacts / "full-verification.json").write_text(json.dumps({"status": audit}))
    called = []
    monkeypatch.setattr(full_project, "execute", lambda command: called.append(command))
    with pytest.raises(RuntimeError, match="refusing model promotion"):
        full_project.main()
    assert not called
    status = json.loads((artifacts / "full-project-status.json").read_text())
    assert status["status"] == "failed" and status["complete"] is False


def test_status_never_starts_work(monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["full_project", "--status"])
    called = []
    monkeypatch.setattr(full_project, "execute", lambda command: called.append(command))
    full_project.main()
    assert not called
    assert json.loads(capsys.readouterr().out)["project"]["status"] == "not_started"


def test_duplicate_start_preserves_the_running_jobs_status(monkeypatch, tmp_path):
    from urbanlake.full_run import pipeline_lock

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["full_project"])
    status = tmp_path / "artifacts/full-project-status.json"
    status.parent.mkdir()
    existing = {"status": "running", "complete": False, "owner": "original job"}
    status.write_text(json.dumps(existing))
    called = []
    monkeypatch.setattr(full_project, "execute", lambda command: called.append(command))
    with pipeline_lock(tmp_path / "data/full/backfill"):
        with pytest.raises(RuntimeError, match="already running"):
            full_project.main()
    assert not called
    assert json.loads(status.read_text()) == existing


def serving_client(monkeypatch, tmp_path, *, stale_attempts=0, observed_count=23008706):
    monkeypatch.chdir(tmp_path)
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    model_bytes = b"full-model-fixture"
    (artifacts / "full-model.joblib").write_bytes(model_bytes)
    expected = hashlib.sha256(model_bytes).hexdigest()[:16]
    (artifacts / "full-model.metrics.json").write_text(json.dumps({"run_id": "full-run"}))
    (artifacts / "full-verification.json").write_text(
        json.dumps(
            {
                "status": "passed",
                "bronze_rows": 23008706,
                "silver_rows": 23008706,
            }
        )
    )
    calls = {"model": 0, "query": 0, "sleep": 0}

    def handler(request):
        path = request.url.path
        if path == "/v1/model":
            calls["model"] += 1
            return httpx.Response(
                200,
                json={
                    "model_version": "old-model" if calls["model"] <= stale_attempts else expected,
                    "supported_target_date": "2026-11-01",
                },
            )
        if path == "/v1/predictions/demand":
            return httpx.Response(200, json={"cached": True, "predicted_requests": 10000})
        if path == "/api/2.0/mlflow/runs/get":
            return httpx.Response(200, json={"run": {"info": {"status": "FINISHED"}}})
        if path == "/api/v1/targets":
            return httpx.Response(
                200,
                json={
                    "data": {
                        "activeTargets": [
                            {"health": "up"},
                            {"health": "up"},
                        ]
                    }
                },
            )
        if path == "/api/v1/query":
            calls["query"] += 1
            assert 'job="urbanlake-pipeline"' in request.url.params["query"]
            return httpx.Response(
                200,
                json={
                    "data": {
                        "result": [
                            {"value": [0, str(observed_count)]},
                        ]
                    }
                },
            )
        raise AssertionError(f"unexpected service request: {request.url}")

    original = httpx.Client
    monkeypatch.setattr(
        full_project.httpx,
        "Client",
        lambda **kwargs: original(
            transport=httpx.MockTransport(handler),
            **kwargs,
        ),
    )

    def sleep(_):
        calls["sleep"] += 1

    monkeypatch.setattr(full_project.time, "sleep", sleep)
    return calls, expected


def test_serving_waits_for_transient_old_model_and_verifies_full_counts(monkeypatch, tmp_path):
    calls, expected = serving_client(monkeypatch, tmp_path, stale_attempts=2)
    result = full_project.verify_serving()
    assert result["model"]["model_version"] == expected
    assert calls == {"model": 3, "query": 2, "sleep": 2}
    assert result["monitoring_counts"] == {
        "bronze_observations": 23008706,
        "silver_requests": 23008706,
    }


def test_serving_rejects_persistently_stale_model_after_bounded_retry(monkeypatch, tmp_path):
    calls, expected = serving_client(monkeypatch, tmp_path, stale_attempts=99)
    with pytest.raises(RuntimeError, match=f"expected {expected}, observed old-model"):
        full_project.verify_serving()
    assert calls == {"model": 30, "query": 0, "sleep": 29}


def test_healthy_targets_cannot_hide_old_sample_metrics(monkeypatch, tmp_path):
    calls, _ = serving_client(monkeypatch, tmp_path, observed_count=320)
    with pytest.raises(RuntimeError, match="full row counts differ"):
        full_project.verify_serving()
    assert calls == {"model": 1, "query": 40, "sleep": 19}
