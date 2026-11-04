import gzip
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest


@pytest.fixture
def packer(monkeypatch):
    path = Path(__file__).resolve().parents[1] / "scripts/prepare_public_release.py"
    spec = importlib.util.spec_from_file_location("urbanlake_public_release_tests", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def git_revision(command, *, cwd, text):
        assert command == ["git", "rev-parse", "HEAD"]
        assert Path(cwd).is_dir() and text is True
        return "0123456789abcdef0123456789abcdef01234567\n"

    monkeypatch.setattr(module.subprocess, "check_output", git_revision)
    return module


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


@pytest.fixture
def workspace(tmp_path, packer):
    root = tmp_path / "workspace"
    root.mkdir()
    model = b"trusted-model-fixture-for-packaging-only"
    digest = hashlib.sha256(model).hexdigest()
    artifacts = root / "artifacts"
    artifacts.mkdir()
    (artifacts / "full-model.joblib").write_bytes(model)
    write_json(
        artifacts / "full-project-status.json",
        {
            "status": "complete",
            "complete": True,
            "stage": "complete",
            "finished_at": "2026-01-01T01:00:00+00:00",
            "internal_secret": "DO_NOT_UPLOAD_PROJECT_SECRET",
            "result": {"model": {"model_version": digest[:16]}},
        },
    )
    write_json(
        artifacts / "full-verification.json",
        {
            "status": "passed",
            "bronze_rows": 3,
            "silver_rows": 3,
            "source_reference_count": 3,
        },
    )
    write_json(
        artifacts / "full-source-probe.json",
        {
            "summary": [{"rows": "3"}],
            "internal_secret": "DO_NOT_UPLOAD_PROBE_SECRET",
        },
    )
    source = root / "data/full/backfill"
    write_json(source / "plan.json", {"partitions": [{"partition_id": "2020-01"}]})
    write_json(
        source / "pipeline.json",
        {
            "status": "complete",
            "complete": True,
            "started_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-01T01:00:00+00:00",
            "verification": {"complete": True},
            "partitions": {"2020-01": {"complete": True}},
        },
    )
    write_json(source / "partitions/2020-01/state.json", {"records": 3, "complete": True})
    for name in packer.SOURCE_FILES:
        path = root / "src/urbanlake" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"allowlisted source: {name}", encoding="utf-8")
    for name in packer.CONFIG_FILES:
        path = root / "deploy/vercel" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"allowlisted config: {name}", encoding="utf-8")
    (root / ".env").write_text("TOKEN=DO_NOT_UPLOAD_ENV_SECRET", encoding="utf-8")
    (artifacts / "unrelated-private.txt").write_text("DO_NOT_UPLOAD_OTHER_ARTIFACT")
    (root / "src/urbanlake/unlisted_private.py").write_text("DO_NOT_UPLOAD_PRIVATE_MODULE")
    write_json(root / "data/raw/request.json", {"address": "DO_NOT_UPLOAD_RAW_ADDRESS"})
    return root


def test_release_is_allowlisted_and_captures_completed_normalized_progress(workspace, packer):
    destination = workspace / "artifacts/public-release"
    result = packer.prepare(workspace, destination)
    expected = {
        *packer.CONFIG_FILES,
        *("src/urbanlake/" + name for name in packer.SOURCE_FILES),
        "assets/full-model.joblib.gz",
        "assets/progress.json",
        "assets/verification.json",
        "assets/release.json",
    }
    actual = {
        path.relative_to(destination).as_posix()
        for path in destination.rglob("*")
        if path.is_file()
    }
    assert actual == expected
    for path in destination.rglob("*"):
        if path.is_file():
            assert b"DO_NOT_UPLOAD" not in path.read_bytes()
    assert (
        gzip.decompress((destination / "assets/full-model.joblib.gz").read_bytes())
        == (workspace / "artifacts/full-model.joblib").read_bytes()
    )
    progress = json.loads((destination / "assets/progress.json").read_text())
    assert progress["mode"] == "verified_snapshot"
    assert progress["status"] == "complete" and progress["overall_percent"] == 100
    assert progress["downloaded_records"] == progress["processed_records"] == 3
    assert progress["completed_months"] == progress["total_months"] == 1
    assert progress["elapsed_seconds"] == 3600  # Frozen at completion, not packager runtime.
    assert progress["captured_at"] == result["captured_at"]
    manifest = json.loads((destination / "assets/release.json").read_text())
    assert "assets/release.json" not in manifest["files"]  # No circular self-hash.
    assert manifest["payload_bytes_excluding_manifest"] == sum(
        data["bytes"] for data in manifest["files"].values()
    )
    assert result["total_upload_bytes"] == sum(
        path.stat().st_size for path in destination.rglob("*") if path.is_file()
    )
    for relative, recorded in result["files"].items():
        payload = (destination / relative).read_bytes()
        assert recorded == {"bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}


@pytest.mark.parametrize("change", ["incomplete_project", "failed_audit", "different_model"])
def test_release_refuses_incomplete_or_mismatched_evidence(workspace, packer, change):
    artifacts = workspace / "artifacts"
    if change == "incomplete_project":
        state = json.loads((artifacts / "full-project-status.json").read_text())
        state.update(complete=False, status="running")
        write_json(artifacts / "full-project-status.json", state)
    elif change == "failed_audit":
        write_json(artifacts / "full-verification.json", {"status": "failed"})
    else:
        (artifacts / "full-model.joblib").write_bytes(b"another-model")
    destination = artifacts / "refused-release"
    with pytest.raises(ValueError):
        packer.prepare(workspace, destination)
    assert not destination.exists()


def test_incomplete_normalized_snapshot_prevents_release(workspace, packer, monkeypatch):
    seen = []

    def incomplete(root):
        seen.append(root)
        return {"overall_percent": 99, "status": "running"}

    monkeypatch.setattr(packer, "snapshot", incomplete)
    destination = workspace / "artifacts/incomplete-progress"
    with pytest.raises(ValueError, match="progress evidence"):
        packer.prepare(workspace, destination)
    assert seen == [workspace.resolve()]
    assert not destination.exists()


@pytest.mark.parametrize("relative", ["public", "artifacts/../public"])
def test_release_destination_cannot_escape_artifacts(workspace, packer, relative):
    destination = workspace / relative
    with pytest.raises(ValueError, match="inside workspace artifacts"):
        packer.prepare(workspace, destination)
    assert not destination.exists()


def test_existing_destination_is_not_reused_or_overwritten(workspace, packer):
    destination = workspace / "artifacts/existing-release"
    destination.mkdir()
    marker = destination / ".env"
    marker.write_text("preserve this existing directory")
    with pytest.raises(FileExistsError):
        packer.prepare(workspace, destination)
    assert marker.read_text() == "preserve this existing directory"
    assert list(destination.iterdir()) == [marker]
