"""Create an allowlisted inference release, excluding raw source data and secrets."""

import gzip
import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from urbanlake.analytics import validate_artifact
from urbanlake.progress import snapshot
from urbanlake.project_summary import build as project_overview

SOURCE_FILES = (
    "__init__.py",
    "api.py",
    "ml.py",
    "progress.py",
    "analytics.py",
    "project_summary.py",
    "static/progress.html",
    "static/app.html",
)
CONFIG_FILES = ("app.py", "pyproject.toml", "vercel.json", ".python-version", "build.py")
ANALYTICS_FIELDS = {
    "schema_version",
    "captured_at",
    "source_start",
    "source_end",
    "source_records",
    "excluded_latest_source_day",
    "months",
    "boroughs",
    "categories",
    "facts",
    "daily",
    "semantics",
}


def prepare(workspace: Path, destination: Path):
    workspace = workspace.resolve()
    destination = destination.resolve()
    if not destination.is_relative_to(workspace / "artifacts"):
        raise ValueError("release destination must be inside workspace artifacts")
    state = json.loads((workspace / "artifacts/full-project-status.json").read_text())
    audit = json.loads((workspace / "artifacts/full-verification.json").read_text())
    if not state.get("complete") or state.get("status") != "complete":
        raise ValueError("only a verified complete project may be released")
    if audit.get("status") != "passed":
        raise ValueError("full-data audit has not passed")
    analytics_bytes = (workspace / "artifacts/public-analytics.json.gz").read_bytes()
    analytics = json.loads(gzip.decompress(analytics_bytes))
    if set(analytics) != ANALYTICS_FIELDS:
        raise ValueError("public analytics contain unreviewed fields")
    validate_artifact(analytics)
    expected = audit["silver_rows"]
    if (
        analytics.get("schema_version") != 1
        or analytics.get("source_records") != expected
        or sum(row[3] for row in analytics["facts"]) != expected
        or sum(row[1] for row in analytics["daily"]) != expected
    ):
        raise ValueError("public analytics do not reconcile with the audited full dataset")
    model = (workspace / "artifacts/full-model.joblib").read_bytes()
    digest = hashlib.sha256(model).hexdigest()
    if digest[:16] != state["result"]["model"]["model_version"]:
        raise ValueError("serving verification belongs to a different model")
    progress = snapshot(workspace)
    if progress["overall_percent"] != 100 or progress["status"] != "complete":
        raise ValueError("progress evidence is incomplete")
    # A new directory prevents stale or unreviewed files from entering a release.
    destination.mkdir(parents=True, exist_ok=False)
    captured = datetime.now(UTC).isoformat()
    progress.update(mode="verified_snapshot", captured_at=captured)
    files = {}

    def write(relative: str, payload: bytes):
        path = destination / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        files[relative] = {"bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}

    for name in CONFIG_FILES:
        write(name, (workspace / "deploy/vercel" / name).read_bytes())
    for name in SOURCE_FILES:
        write("src/urbanlake/" + name, (workspace / "src/urbanlake" / name).read_bytes())
    write("assets/full-model.joblib.gz", gzip.compress(model, compresslevel=9, mtime=0))
    write("assets/progress.json", json.dumps(progress, indent=2).encode())
    write("assets/verification.json", json.dumps(audit, indent=2).encode())
    write("assets/analytics.json.gz", analytics_bytes)
    write(
        "assets/project.json",
        json.dumps(
            project_overview(workspace, captured_at=captured, mode="hosted_app"), indent=2
        ).encode(),
    )
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=workspace, text=True).strip()
    manifest = {
        "captured_at": captured,
        "source_commit": commit,
        "model_sha256": digest,
        "model_version": digest[:16],
        "scope": "hosted analytics application, model inference, and verified project evidence",
        "local_only": ["raw requests", "Kafka", "Spark/Delta", "Airflow", "MLflow"],
        "files": files.copy(),
        "payload_bytes_excluding_manifest": sum(item["bytes"] for item in files.values()),
    }
    write("assets/release.json", json.dumps(manifest, indent=2).encode())
    return {
        **manifest,
        "directory": str(destination),
        "files": files,
        "total_upload_bytes": sum(item["bytes"] for item in files.values()),
    }


if __name__ == "__main__":
    root = Path.cwd()
    target = root / "artifacts" / ("public-release-" + datetime.now(UTC).strftime("%Y%m%d-%H%M%S"))
    print(json.dumps(prepare(root, target), indent=2))
