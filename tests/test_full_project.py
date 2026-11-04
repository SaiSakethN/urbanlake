import json

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
