"""Read-only progress snapshots from durable job state, never simulated growth."""

import json
from datetime import UTC, datetime
from pathlib import Path


def read(path: Path, default=None):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def percentage(count, total):
    return round(min(100.0, max(0.0, count / total * 100)), 2) if total else 0.0


def snapshot(workspace: Path = Path(".")):
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
    elapsed = max(0, (now - datetime.fromisoformat(started)).total_seconds()) if started else 0
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
