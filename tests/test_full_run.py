import json
import subprocess
import threading
from argparse import Namespace

import pytest

from urbanlake.full_run import container_path, docker_command, load_pipeline, run, run_stage


def arguments(root, lake):
    return Namespace(
        root=root,
        lake=lake,
        checkpoint=lake / "checkpoint",
        topic="full-test",
        max_partitions=None,
        page_size=5000,
        min_free_gb=0,
        max_offsets=100000,
        attempts=1,
    )


def test_stage_retries_only_until_success(tmp_path):
    calls = []

    def runner(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 1 if len(calls) == 1 else 0)

    run_stage(
        ["safe", "argument with spaces"],
        log=tmp_path / "log.txt",
        workspace=tmp_path,
        attempts=3,
        runner=runner,
        sleep=lambda _: None,
    )
    assert len(calls) == 2
    assert calls[0] == ["safe", "argument with spaces"]


def test_failure_does_not_claim_success(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    root = tmp_path / "source"
    root.mkdir()
    plan = {
        "run_id": "test",
        "start": "2020-01-01",
        "end": "2020-02-01",
        "partitions": [{"partition_id": "2020-01"}],
    }
    (root / "plan.json").write_text(json.dumps(plan))

    def failed(command, **kwargs):
        return subprocess.CompletedProcess(command, 7)

    with pytest.raises(RuntimeError, match="stage failed"):
        run(arguments(root, tmp_path / "lake"), runner=failed, sleep=lambda _: None)
    result = json.loads((root / "pipeline.json").read_text())
    assert result["status"] == "failed"
    assert result["complete"] is False
    assert not result["partitions"]["2020-01"]["stages"]


def test_pipeline_identity_cannot_be_reused_for_another_topic(tmp_path):
    args = arguments(tmp_path, tmp_path / "lake")
    plan = {"run_id": "test", "start": "2020-01-01", "end": "2020-02-01"}
    state = load_pipeline(args, plan)
    (tmp_path / "pipeline.json").write_text(json.dumps(state))
    args.topic = "different"
    with pytest.raises(ValueError, match="identity changed"):
        load_pipeline(args, plan)


def test_docker_bind_arguments_are_not_shell_text(tmp_path):
    command = docker_command(tmp_path / "path with spaces", ["-m", "urbanlake.bulk_processing"])
    assert "shell" not in command
    assert f"{tmp_path / 'path with spaces' / 'src'}:/app/src:ro" in command
    with pytest.raises(ValueError, match="workspace"):
        container_path(tmp_path, tmp_path.parent / "outside")


def test_resume_skips_verified_finished_partition(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    root = tmp_path / "source"
    root.mkdir()
    plan = {
        "run_id": "test",
        "start": "2020-01-01",
        "end": "2020-02-01",
        "partitions": [{"partition_id": "2020-01"}],
    }
    (root / "plan.json").write_text(json.dumps(plan))
    args = arguments(root, tmp_path / "lake")
    state = load_pipeline(args, plan)
    state["partitions"]["2020-01"] = {"complete": True, "stages": {}}
    state["verification"] = {"complete": True}
    (root / "pipeline.json").write_text(json.dumps(state))

    def forbidden(*_, **__):
        pytest.fail("completed partition should not rerun")

    result = run(args, runner=forbidden)
    assert result["complete"]
    assert result["completed_partitions"] == 1


def test_prefetch_overlaps_processing_with_only_one_downloader(tmp_path, monkeypatch):
    import hashlib

    monkeypatch.chdir(tmp_path)
    root, lake = tmp_path / "source", tmp_path / "lake"
    root.mkdir()
    (root / "plan.json").write_text(
        json.dumps(
            {
                "run_id": "test",
                "start": "2020-01-01",
                "end": "2020-03-01",
                "partitions": [{"partition_id": "2020-01"}, {"partition_id": "2020-02"}],
            }
        )
    )
    args = arguments(root, lake)
    args.prefetch = True
    prefetched_started = threading.Event()
    processing_started = threading.Event()
    counter_lock = threading.Lock()
    active = peak = 0
    calls = []

    def fake(command, **kwargs):
        nonlocal active, peak
        calls.append(command)
        partition = command[command.index("--partition") + 1] if "--partition" in command else None
        directory = root / "partitions" / partition if partition else None
        if "urbanlake.backfill" in command:
            with counter_lock:
                active += 1
                peak = max(peak, active)
            if partition == "2020-02":
                prefetched_started.set()
                assert processing_started.wait(timeout=5), "prefetch did not overlap processing"
            directory.mkdir(parents=True)
            (directory / "state.json").write_text(json.dumps({"complete": True, "records": 1}))
            with counter_lock:
                active -= 1
        elif "urbanlake.bulk_replay" in command:
            (directory / "kafka.json").write_text(json.dumps({"complete": True, "records": 1}))
        elif "consume-finalize" in command:
            if partition == "2020-01":
                assert prefetched_started.wait(timeout=5)
                assert active == 1
                processing_started.set()
            source = json.loads((directory / "state.json").read_text())
            marker = lake / "_processing_state" / f"{partition}.json"
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(
                json.dumps(
                    {
                        "status": "complete",
                        "source_manifest_sha256": hashlib.sha256(
                            json.dumps(source, sort_keys=True).encode()
                        ).hexdigest(),
                    }
                )
            )
        elif "/app/scripts/verify_full.py" in command:
            (tmp_path / "artifacts").mkdir()
            (tmp_path / "artifacts/full-verification.json").write_text('{"status":"passed"}')
        else:
            pytest.fail(f"unexpected command: {command}")
        return subprocess.CompletedProcess(command, 0)

    result = run(args, runner=fake)
    state = json.loads((root / "pipeline.json").read_text())
    assert result["complete"]
    assert peak == 1
    assert state["partitions"]["2020-02"]["stages"]["download"]["prefetched"]
    assert not any("finalize" in command for command in calls)


def test_one_partition_pilot_does_not_prefetch_beyond_its_budget(tmp_path, monkeypatch):
    import hashlib

    monkeypatch.chdir(tmp_path)
    root, lake = tmp_path / "source", tmp_path / "lake"
    root.mkdir()
    (root / "plan.json").write_text(
        json.dumps(
            {
                "run_id": "test",
                "start": "2020-01-01",
                "end": "2020-03-01",
                "partitions": [{"partition_id": "2020-01"}, {"partition_id": "2020-02"}],
            }
        )
    )
    args = arguments(root, lake)
    args.prefetch, args.max_partitions = True, 1
    calls = []

    def fake(command, **kwargs):
        calls.append(command)
        partition = command[command.index("--partition") + 1]
        assert partition == "2020-01", "pilot touched an unapproved partition budget"
        directory = root / "partitions" / partition
        directory.mkdir(parents=True, exist_ok=True)
        if "urbanlake.backfill" in command:
            (directory / "state.json").write_text(json.dumps({"complete": True, "records": 1}))
        elif "urbanlake.bulk_replay" in command:
            (directory / "kafka.json").write_text(json.dumps({"complete": True, "records": 1}))
        else:
            source = json.loads((directory / "state.json").read_text())
            marker = lake / "_processing_state" / f"{partition}.json"
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(
                json.dumps(
                    {
                        "status": "complete",
                        "source_manifest_sha256": hashlib.sha256(
                            json.dumps(source, sort_keys=True).encode()
                        ).hexdigest(),
                    }
                )
            )
        return subprocess.CompletedProcess(command, 0)

    result = run(args, runner=fake)
    assert not result["complete"]
    assert result["status"] == "paused_at_partition_budget"
    assert len(calls) == 3


def test_prefetch_failure_leaves_next_month_incomplete(tmp_path, monkeypatch):
    import hashlib

    monkeypatch.chdir(tmp_path)
    root, lake = tmp_path / "source", tmp_path / "lake"
    root.mkdir()
    (root / "plan.json").write_text(
        json.dumps(
            {
                "run_id": "test",
                "start": "2020-01-01",
                "end": "2020-03-01",
                "partitions": [{"partition_id": "2020-01"}, {"partition_id": "2020-02"}],
            }
        )
    )
    args = arguments(root, lake)
    args.prefetch = True

    def fake(command, **kwargs):
        partition = command[command.index("--partition") + 1]
        if partition == "2020-02":
            return subprocess.CompletedProcess(command, 9)
        directory = root / "partitions" / partition
        directory.mkdir(parents=True, exist_ok=True)
        if "urbanlake.backfill" in command:
            (directory / "state.json").write_text(json.dumps({"complete": True, "records": 1}))
        elif "urbanlake.bulk_replay" in command:
            (directory / "kafka.json").write_text(json.dumps({"complete": True, "records": 1}))
        else:
            source = json.loads((directory / "state.json").read_text())
            marker = lake / "_processing_state" / f"{partition}.json"
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(
                json.dumps(
                    {
                        "status": "complete",
                        "source_manifest_sha256": hashlib.sha256(
                            json.dumps(source, sort_keys=True).encode()
                        ).hexdigest(),
                    }
                )
            )
        return subprocess.CompletedProcess(command, 0)

    with pytest.raises(RuntimeError, match="stage failed"):
        run(args, runner=fake, sleep=lambda _: None)
    state = json.loads((root / "pipeline.json").read_text())
    assert state["status"] == "failed" and not state["complete"]
    assert state["partitions"]["2020-01"]["complete"]
    assert not state["partitions"]["2020-02"]["complete"]
    assert state["current_partition"] == "2020-02"
    assert state["current_stage"] == "download"
