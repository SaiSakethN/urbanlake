"""Read-only progress snapshots from durable job state, never simulated growth."""

import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path


def read(path: Path, default=None):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def percentage(count, total):
    return round(min(100.0, max(0.0, count / total * 100)), 2) if total else 0.0


def verified_snapshot(path: Path):
    """A hosted report is frozen evidence, not a live remote lakehouse connection."""
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("verified snapshot must be a JSON object")
    if data.get("status") != "complete" or data.get("phase") != "complete" or data.get("error"):
        raise ValueError("hosted progress requires a completed verified run")
    for name in ("download_percent", "processing_percent", "overall_percent"):
        if data.get(name) != 100:
            raise ValueError("hosted progress requires completed percentages")
    counts = [
        data.get(name) for name in ("total_records", "downloaded_records", "processed_records")
    ]
    if any(type(value) is not int or value <= 0 for value in counts) or len(set(counts)) != 1:
        raise ValueError("hosted progress source and processed counts do not agree")
    months = data.get("months")
    if not isinstance(months, list) or not months or any(not isinstance(m, dict) for m in months):
        raise ValueError("hosted progress requires completed source months")
    labels = [month.get("month") for month in months]
    if any(
        not isinstance(label, str) or not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", label)
        for label in labels
    ) or len(labels) != len(set(labels)):
        raise ValueError("hosted source months must be valid and unique")
    if data.get("completed_months") != len(months) or data.get("total_months") != len(months):
        raise ValueError("hosted progress month coverage is incomplete")
    if any(
        month.get("state") != "done"
        or type(month.get("records")) is not int
        or month["records"] < 0
        for month in months
    ):
        raise ValueError("hosted progress contains an incomplete source month")
    if sum(month["records"] for month in months) != counts[0]:
        raise ValueError("hosted source-month totals do not match the verified record count")
    phases = data.get("phases")
    required = {"download", "process", "verify", "train", "serve"}
    if (
        not isinstance(phases, list)
        or len(phases) != len(required)
        or any(
            not isinstance(phase, dict)
            or not isinstance(phase.get("key"), str)
            or phase.get("state") != "done"
            or phase.get("percent") != 100
            for phase in phases
        )
        or {phase.get("key") for phase in phases} != required
    ):
        raise ValueError("hosted progress requires all verification/training/serving phases")
    captured = data.get("captured_at", data.get("snapshot_at"))
    for value in (data.get("snapshot_at"), captured):
        if not isinstance(value, str) or datetime.fromisoformat(value).tzinfo is None:
            raise ValueError("hosted progress requires aware capture timestamps")
    return {**data, "mode": "verified_snapshot", "captured_at": captured}


def snapshot(workspace: Path = Path(".")):
    configured = os.getenv("PROGRESS_SNAPSHOT_PATH")
    if configured is not None:
        if not configured.strip():
            raise ValueError("configured progress snapshot path is empty")
        path = Path(configured)
        return verified_snapshot(path if path.is_absolute() else workspace / path)
    root = workspace / "data/full/backfill"
    plan = read(root / "plan.json", {})
    pipeline = read(root / "pipeline.json", {})
    project = read(workspace / "artifacts/full-project-status.json", {})
    reference = read(workspace / "artifacts/full-source-probe.json", {})
    total = int((reference.get("summary") or [{}])[0].get("rows", 0))
    status = project.get("status", pipeline.get("status", "not_started"))
    if status not in {"running", "failed", "complete", "not_started"}:
        status = "running"
    current = pipeline.get("current_partition")
    phase = project.get("stage", "pipeline")
    stage = phase if phase in {"training", "serving", "complete"} else pipeline.get("current_stage")
    entries = pipeline.get("partitions", {})
    downloaded = processed = done = 0
    months = []
    download_done = bool(plan.get("partitions"))
    for partition in plan.get("partitions", []):
        month = partition["partition_id"]
        source = read(root / "partitions" / month / "state.json", {})
        count = int(source.get("records", 0))
        downloaded += count
        finalized = bool(entries.get(month, {}).get("complete"))
        if finalized:
            processed += count
            done += 1
            state = "done"
        elif month == current:
            state = "failed" if status == "failed" else "active"
        elif source.get("complete"):
            state = "downloaded"
        elif count:
            state = "active"
        else:
            state = "pending"
        download_done = download_done and bool(source.get("complete"))
        months.append({"month": month, "state": state, "records": count})
    verify_done = bool(pipeline.get("verification", {}).get("complete"))
    train_done = phase in {"serving", "complete"}
    serve_done = status == "complete" and bool(project.get("complete"))
    dp, pp = percentage(downloaded, total), percentage(processed, total)
    overall = dp * 0.3 + pp * 0.6 + 4 * verify_done + 4 * train_done + 2 * serve_done
    definitions = [
        ("download", "Download history", download_done, dp),
        ("process", "Build lakehouse", bool(months) and done == len(months), pp),
        ("verify", "Audit complete data", verify_done, 100.0 if verify_done else 0.0),
        ("train", "Train full model", train_done, 100.0 if train_done else 0.0),
        ("serve", "Verify serving", serve_done, 100.0 if serve_done else 0.0),
    ]
    active = (
        "serve"
        if phase == "serving"
        else "train"
        if phase == "training"
        else "verify"
        if stage == "verify-full"
        else "process"
    )
    phases = []
    for key, label, complete, percent in definitions:
        state = "done" if complete else "pending"
        if not complete and status != "not_started":
            if key == active or key == "download" and active == "process":
                state = "failed" if status == "failed" and key == active else "running"
        if not complete and status == "failed" and stage == "download":
            state = "failed" if key == "download" else "pending"
        phases.append({"key": key, "label": label, "state": state, "percent": percent})
    now = datetime.now(UTC)
    started = pipeline.get("started_at", project.get("started_at"))
    finished = project.get("finished_at", pipeline.get("finished_at")) if serve_done else None
    clock_end = datetime.fromisoformat(finished) if finished else now
    elapsed = (
        max(0, (clock_end - datetime.fromisoformat(started)).total_seconds()) if started else 0
    )
    remaining = (elapsed / done * (len(months) - done)) if done and done < len(months) else None
    return {
        "status": status,
        "phase": phase,
        "total_records": total,
        "downloaded_records": downloaded,
        "processed_records": processed,
        "download_percent": dp,
        "processing_percent": pp,
        "overall_percent": 100.0 if serve_done else round(min(overall, 99.0), 2),
        "completed_months": done,
        "total_months": len(months),
        "current_month": current,
        "current_stage": stage,
        "elapsed_seconds": round(elapsed),
        "estimated_remaining_seconds": round(remaining) if remaining else None,
        "updated_at": pipeline.get("updated_at", project.get("finished_at", started)),
        "snapshot_at": now.isoformat(),
        "phases": phases,
        "months": months,
        "error": "The run needs attention. Its stage log contains the error."
        if status == "failed"
        else None,
    }
