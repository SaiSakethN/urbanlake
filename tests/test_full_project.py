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
