"""Resumable foreground coordination of the full source -> Kafka -> Delta run.

The immutable source plan fixes scope. A stage is recorded only after its process
and evidence checks succeed. Restarting the same command resumes failed work.
"""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from urbanlake.bulk_replay import atomic_json

STAGES = ("download", "relay", "consume", "finalize")


def utc_now():
    return datetime.now(UTC).isoformat()


@contextmanager
def pipeline_lock(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".pipeline.lock").open("a+b") as handle:
        if handle.seek(0, os.SEEK_END) == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise RuntimeError("another full-pipeline process owns this source root") from error
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def check_disk(root: Path, min_free_gb: float):
    if min_free_gb < 0:
        raise ValueError("min-free-gb must be nonnegative")
    free = shutil.disk_usage(root).free
    if free < min_free_gb * 1024**3:
        raise RuntimeError(f"full pipeline stopped at disk reserve: {free / 1024**3:.2f} GiB free")


def docker_command(workspace: Path, module_arguments: list[str]) -> list[str]:
    """No shell interpolation; bind paths are absolute and stay inside workspace."""
    return [
        "docker",
        "compose",
        "run",
        "--rm",
        "--no-deps",
        "--entrypoint",
        "python",
        "-e",
        "PYTHONPATH=/app/src",
        "-v",
        f"{workspace / 'src'}:/app/src:ro",
        "-v",
        f"{workspace / 'sql'}:/app/sql:ro",
        "-v",
        f"{workspace / 'scripts'}:/app/scripts:ro",
        "-v",
        "urbanlake-ivy:/root/.ivy2.5.2",
        "spark",
        *module_arguments,
    ]


def container_path(workspace: Path, path: Path) -> str:
    resolved = path.resolve()
    if not resolved.is_relative_to(workspace.resolve()):
        raise ValueError("source and lake paths must stay inside the workspace")
    return "/app/" + resolved.relative_to(workspace.resolve()).as_posix()


def stage_command(stage: str, partition: str, args, workspace: Path) -> list[str]:
    source_root = str(args.root.resolve())
    if stage == "download":
        return [
            sys.executable,
            "-m",
            "urbanlake.backfill",
            "run",
            "--root",
            source_root,
            "--partition",
            partition,
            "--workers",
            "1",
            "--page-size",
            str(args.page_size),
            "--min-free-gb",
            str(args.min_free_gb),
        ]
    if stage == "relay":
        return [
            sys.executable,
            "-m",
            "urbanlake.bulk_replay",
            "--root",
            source_root,
            "--partition",
            partition,
            "--topic",
            args.topic,
        ]
    common = [
        "--input-root",
        container_path(workspace, args.root),
        "--lake",
        container_path(workspace, args.lake),
    ]
    if stage == "consume":
        command = [
            "-m",
            "urbanlake.bulk_processing",
            "consume-finalize",
            *common,
            "--partition",
            partition,
            "--topic",
            args.topic,
            "--checkpoint",
            container_path(workspace, args.checkpoint),
            "--max-offsets",
            str(args.max_offsets),
        ]
    elif stage == "finalize":
        command = [
            "-m",
            "urbanlake.bulk_processing",
            "finalize",
            *common,
            "--partition",
            partition,
        ]
    else:
        raise ValueError(f"unknown stage {stage}")
    return docker_command(workspace, command)


def validate_stage_evidence(stage: str, partition: str, root: Path, lake: Path | None = None):
    source = root / "partitions" / partition / "state.json"
    if stage in ("download", "relay"):
        state = json.loads(source.read_text(encoding="utf-8"))
        if not state.get("complete"):
            raise RuntimeError(f"partition {partition} source coverage is incomplete")
        if stage == "relay":
            relay = json.loads(source.with_name("kafka.json").read_text(encoding="utf-8"))
            if not relay.get("complete") or relay["records"] != state["records"]:
                raise RuntimeError(f"partition {partition} Kafka relay is incomplete")
    elif stage == "finalize" and lake is not None:
        marker = json.loads((lake / "_processing_state" / f"{partition}.json").read_text())
        manifest = json.loads(source.read_text())
        checksum = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
        if marker.get("status") != "complete" or marker.get("source_manifest_sha256") != checksum:
            raise RuntimeError(f"partition {partition} processing evidence is stale or incomplete")


def run_stage(
    command: list[str],
    *,
    log: Path,
    workspace: Path,
    attempts: int,
    runner=subprocess.run,
    sleep=time.sleep,
):
    if not 1 <= attempts <= 5:
        raise ValueError("attempts must be 1..5")
    log.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, attempts + 1):
        with log.open("a", encoding="utf-8") as output:
            output.write(f"\n{utc_now()} attempt {attempt}\n")
            output.flush()
            result = runner(
                command, cwd=workspace, stdout=output, stderr=subprocess.STDOUT, check=False
            )
        if result.returncode == 0:
            return
        if attempt < attempts:
            sleep(min(2**attempt, 30))
    raise RuntimeError(
        f"stage failed after {attempts} attempts (exit {result.returncode}); log={log}"
    )


class PrefetchJob:
    """One cancellable downloader process, with a testable subprocess boundary."""

    def __init__(
        self,
        partition: str,
        command: list[str],
        *,
        log: Path,
        workspace: Path,
        attempts: int,
        runner,
        sleep,
    ):
        self.partition = partition
        self.command = command
        self.log = log
        self.workspace = workspace
        self.attempts = attempts
        self.runner = runner
        self.sleep = sleep
        self.cancelled = threading.Event()
        self.lock = threading.Lock()
        self.process = None

    def managed_runner(self, command, **kwargs):
        kwargs.pop("check", None)
        with self.lock:
            if self.cancelled.is_set():
                raise RuntimeError("prefetch cancelled")
            self.process = subprocess.Popen(command, **kwargs)
            process = self.process
        code = process.wait()
        return subprocess.CompletedProcess(command, code)

    def execute(self):
        started = time.monotonic()
        execute = self.managed_runner if self.runner is subprocess.run else self.runner
        run_stage(
            self.command,
            log=self.log,
            workspace=self.workspace,
            attempts=self.attempts,
            runner=execute,
            sleep=self.sleep,
        )
        return {"seconds": round(time.monotonic() - started, 3), "finished_at": utc_now()}

    def stop(self):
        self.cancelled.set()
        with self.lock:
            process = self.process
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)


def evidence_exists(stage: str, partition: str, root: Path, lake: Path):
    try:
        validate_stage_evidence(stage, partition, root, lake)
        return True
    except (OSError, ValueError, RuntimeError, KeyError):
        return False


def load_pipeline(args, plan: dict) -> dict:
    identity = {
        "source_run_id": plan["run_id"],
        "start": plan["start"],
        "end": plan["end"],
        "lake": str(args.lake.resolve()),
        "checkpoint": str(args.checkpoint.resolve()),
        "topic": args.topic,
    }
    identity_hash = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    path = args.root / "pipeline.json"
    if path.exists():
        state = json.loads(path.read_text(encoding="utf-8"))
        if state["identity_sha256"] != identity_hash:
            raise ValueError("pipeline identity changed; use a new source root/checkpoint")
        return state
    return {
        "schema_version": 1,
        "identity": identity,
        "identity_sha256": identity_hash,
        "started_at": utc_now(),
        "status": "pending",
        "complete": False,
        "partitions": {},
    }


def progress_report(root: Path, state: dict, plan: dict) -> dict:
    completed = sum(bool(item.get("complete")) for item in state["partitions"].values())
    counts = 0
    for partition in plan["partitions"]:
        path = root / "partitions" / partition["partition_id"] / "state.json"
        if path.exists():
            counts += int(json.loads(path.read_text(encoding="utf-8"))["records"])
    elapsed = (datetime.now(UTC) - datetime.fromisoformat(state["started_at"])).total_seconds()
    remaining = len(plan["partitions"]) - completed
    return {
        "status": state["status"],
        "complete": state["complete"],
        "completed_partitions": completed,
        "total_partitions": len(plan["partitions"]),
        "downloaded_records": counts,
        "elapsed_seconds": round(elapsed, 1),
        "estimated_remaining_seconds": round(elapsed / completed * remaining, 1)
        if completed
        else None,
        "current_partition": state.get("current_partition"),
        "current_stage": state.get("current_stage"),
        "updated_at": utc_now(),
    }


def run(args, *, runner=subprocess.run, sleep=time.sleep):
    workspace = Path.cwd().resolve()
    args.root, args.lake, args.checkpoint = (
        args.root.resolve(),
        args.lake.resolve(),
        args.checkpoint.resolve(),
    )
    for path in (args.root, args.lake, args.checkpoint):
        container_path(workspace, path)
    with pipeline_lock(args.root):
        plan = json.loads((args.root / "plan.json").read_text(encoding="utf-8"))
        state = load_pipeline(args, plan)
        state["status"] = "running"
        state.pop("error", None)
        processed = 0
        executor = ThreadPoolExecutor(max_workers=1) if getattr(args, "prefetch", False) else None
        prefetch_job = None
        prefetch_future = None

        def start_prefetch(index):
            nonlocal prefetch_job, prefetch_future
            if executor is None or prefetch_future is not None:
                return
            if args.max_partitions is not None and processed + 1 >= args.max_partitions:
                return
            if index + 1 >= len(plan["partitions"]):
                return
            target = plan["partitions"][index + 1]["partition_id"]
            if state["partitions"].get(target, {}).get("complete") or evidence_exists(
                "download", target, args.root, args.lake
            ):
                return
            check_disk(args.root, args.min_free_gb)
            prefetch_job = PrefetchJob(
                target,
                stage_command("download", target, args, workspace),
                log=args.root / "logs" / f"{target}-download.log",
                workspace=workspace,
                attempts=args.attempts,
                runner=runner,
                sleep=sleep,
            )
            prefetch_future = executor.submit(prefetch_job.execute)
            print(json.dumps({"event": "prefetch_started", "partition": target}), flush=True)

        try:
            for index, partition in enumerate(plan["partitions"]):
                identifier = partition["partition_id"]
                entry = state["partitions"].setdefault(
                    identifier, {"stages": {}, "complete": False}
                )
                if entry["complete"]:
                    continue
                if args.max_partitions is not None and processed >= args.max_partitions:
                    state["status"] = "paused_at_partition_budget"
                    break
                for stage in STAGES:
                    if entry["stages"].get(stage, {}).get("complete"):
                        validate_stage_evidence(stage, identifier, args.root, args.lake)
                        if stage == "download":
                            start_prefetch(index)
                        continue
                    check_disk(args.root, args.min_free_gb)
                    state.update(
                        current_partition=identifier, current_stage=stage, updated_at=utc_now()
                    )
                    atomic_json(args.root / "pipeline.json", state)
                    print(json.dumps(progress_report(args.root, state, plan)), flush=True)
                    started = time.monotonic()
                    details = {}
                    if stage == "download" and prefetch_future is not None:
                        if prefetch_job.partition != identifier:
                            raise RuntimeError(
                                "prefetch ordering violated; refusing parallel downloaders"
                            )
                        details = {**prefetch_future.result(), "prefetched": True}
                        prefetch_future = None
                        prefetch_job = None
                    elif stage in ("download", "relay", "finalize") and evidence_exists(
                        stage, identifier, args.root, args.lake
                    ):
                        details = {"reused_evidence": True}
                    else:
                        run_stage(
                            stage_command(stage, identifier, args, workspace),
                            log=args.root / "logs" / f"{identifier}-{stage}.log",
                            workspace=workspace,
                            attempts=args.attempts,
                            runner=runner,
                            sleep=sleep,
                        )
                    validate_stage_evidence(stage, identifier, args.root, args.lake)
                    entry["stages"][stage] = {
                        "complete": True,
                        "finished_at": utc_now(),
                        "seconds": round(time.monotonic() - started, 3),
                        **details,
                    }
                    atomic_json(args.root / "pipeline.json", state)
                    if stage == "download":
                        start_prefetch(index)
                entry["complete"] = True
                processed += 1
                atomic_json(args.root / "pipeline.json", state)
            state["complete"] = all(
                state["partitions"].get(part["partition_id"], {}).get("complete", False)
                for part in plan["partitions"]
            )
            if state["complete"] and not state.get("verification", {}).get("complete"):
                state["complete"] = False
                state["status"] = "verifying"
                state["current_stage"] = "verify-full"
                atomic_json(args.root / "pipeline.json", state)
                run_stage(
                    docker_command(
                        workspace,
                        [
                            "/app/scripts/verify_full.py",
                            "--root",
                            container_path(workspace, args.root),
                            "--lake",
                            container_path(workspace, args.lake),
                            "--report",
                            "/app/artifacts/full-verification.json",
                        ],
                    ),
                    log=args.root / "logs" / "verify-full.log",
                    workspace=workspace,
                    attempts=args.attempts,
                    runner=runner,
                    sleep=sleep,
                )
                evidence = json.loads((workspace / "artifacts/full-verification.json").read_text())
                if evidence.get("status") != "passed":
                    raise RuntimeError("full verification did not report passed")
                state["verification"] = {
                    "complete": True,
                    "finished_at": utc_now(),
                    "report": "artifacts/full-verification.json",
                }
                state["complete"] = True
            if state["complete"]:
                state["status"] = "complete"
                state["finished_at"] = utc_now()
        except KeyboardInterrupt:
            state["status"] = "interrupted"
            state["complete"] = False
            raise
        except Exception as error:
            state["status"] = "failed"
            state["complete"] = False
            state["error"] = f"{type(error).__name__}: {error}"
            raise
        finally:
            if prefetch_job is not None:
                prefetch_job.stop()
            if executor is not None:
                executor.shutdown(wait=True, cancel_futures=True)
            state["updated_at"] = utc_now()
            atomic_json(args.root / "pipeline.json", state)
            report = progress_report(args.root, state, plan)
            atomic_json(args.root / "pipeline-status.json", report)
            print(json.dumps(report), flush=True)
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["run", "status"])
    parser.add_argument("--root", type=Path, default=Path("data/full/backfill"))
    parser.add_argument("--lake", type=Path, default=Path("data/full/lake"))
    parser.add_argument("--checkpoint", type=Path, default=Path("data/full/checkpoints/kafka-v1"))
    parser.add_argument("--topic", default="urbanlake.full.observations.v1")
    parser.add_argument("--page-size", type=int, default=5000)
    parser.add_argument("--max-offsets", type=int, default=100000)
    parser.add_argument("--min-free-gb", type=float, default=30)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument(
        "--prefetch",
        action="store_true",
        help="download one next month while current month processes",
    )
    parser.add_argument(
        "--max-partitions", type=int, help="finite pilot; omit for entire planned scope"
    )
    args = parser.parse_args()
    if args.max_partitions is not None and args.max_partitions < 1:
        parser.error("max-partitions must be positive")
    if args.command == "status":
        state = json.loads((args.root / "pipeline.json").read_text())
        plan = json.loads((args.root / "plan.json").read_text())
        print(json.dumps(progress_report(args.root, state, plan), indent=2))
    else:
        run(args)


if __name__ == "__main__":
    main()
