"""Finish full ingestion, independent audit, training and local serving in order.

Every stage is resumable through its underlying state/artifacts. No model is
promoted to the API before global source and lake coverage checks pass.
"""

import argparse
import hashlib
import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx

from urbanlake.bulk_replay import atomic_json

STATUS = Path("artifacts/full-project-status.json")


def execute(command):
    subprocess.run(command, check=True)


def verify_serving():
    with httpx.Client(timeout=10) as client:
        for attempt in range(30):
            try:
                response = client.get("http://localhost:8000/v1/model")
                response.raise_for_status()
                model = response.json()
                expected = hashlib.sha256(
                    Path("artifacts/full-model.joblib").read_bytes()
                ).hexdigest()[:16]
                if model["model_version"] != expected:
                    raise RuntimeError("API has not loaded the newly trained full-history model")
                break
            except httpx.HTTPError:
                if attempt == 29:
                    raise
                time.sleep(2)
        payload = {"schema_version": 1, "target_date": model["supported_target_date"]}
        for _ in range(2):
            forecast = client.post("http://localhost:8000/v1/predictions/demand", json=payload)
            forecast.raise_for_status()
        if not forecast.json()["cached"]:
            raise RuntimeError("full-model prediction did not verify actual Redis caching")
        training = json.loads(Path("artifacts/full-model.metrics.json").read_text())
        tracked = client.get(
            "http://localhost:5000/api/2.0/mlflow/runs/get",
            params={
                "run_id": training["run_id"],
            },
        )
        tracked.raise_for_status()
        if tracked.json()["run"]["info"]["status"] != "FINISHED":
            raise RuntimeError("full-history training run is not FINISHED in MLflow")
        # Scrape interval is fifteen seconds; bounded polling verifies live metrics.
        for attempt in range(20):
            try:
                targets = client.get("http://localhost:9090/api/v1/targets").json()
                active = targets["data"]["activeTargets"]
                if len(active) == 2 and all(t["health"] == "up" for t in active):
                    break
            except (httpx.HTTPError, KeyError, ValueError):
                pass
            if attempt == 19:
                raise RuntimeError("full-history monitoring targets are not healthy")
            time.sleep(2)
        return {"model": model, "prediction": forecast.json(), "training": training}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--status", action="store_true", help="read live progress without starting work"
    )
    parser.add_argument(
        "--skip-pipeline", action="store_true", help="resume after global audit only"
    )
    args, pipeline_arguments = parser.parse_known_args()
    if args.status:
        from urbanlake.full_run import progress_report

        project = json.loads(STATUS.read_text()) if STATUS.exists() else {"status": "not_started"}
        root = Path("data/full/backfill")
        pipeline = None
        if (root / "pipeline.json").exists():
            state = json.loads((root / "pipeline.json").read_text())
            plan = json.loads((root / "plan.json").read_text())
            pipeline = progress_report(root, state, plan)
        print(json.dumps({"project": project, "pipeline": pipeline}, indent=2))
        return
    state = {"status": "running", "complete": False, "started_at": datetime.now(UTC).isoformat()}
    try:
        atomic_json(STATUS, state)
        if not args.skip_pipeline:
            execute([sys.executable, "-m", "urbanlake.full_run", "run", *pipeline_arguments])
        pipeline = json.loads(Path("data/full/backfill/pipeline.json").read_text())
        audit = json.loads(Path("artifacts/full-verification.json").read_text())
        if not pipeline.get("complete") or audit.get("status") != "passed":
            raise RuntimeError("full source/lake audit has not passed; refusing model promotion")
        state["stage"] = "training"
        atomic_json(STATUS, state)
        execute(["docker", "compose", "up", "-d", "--wait", "--wait-timeout", "120", "mlflow"])
        execute(
            [
                "docker",
                "compose",
                "run",
                "--rm",
                "train",
                "train",
                "--input",
                "data/full/lake/gold/demand_export",
                "--output",
                "artifacts/full-model.joblib",
            ]
        )
        metrics = {
            "bronze_observations": audit["bronze_rows"],
            "silver_requests": audit["silver_rows"],
            "daily_demand_days": audit["daily_demand_days"],
            "daily_demand_feature_rows": audit["daily_demand_feature_rows"],
            "observed_changes": audit["observed_changes"],
            "invalid_requests": audit["invalid_requests"],
            "last_success_unixtime": datetime.now(UTC).timestamp(),
        }
        atomic_json(Path("artifacts/full-pipeline-metrics.json"), metrics)
        state["stage"] = "serving"
        atomic_json(STATUS, state)
        execute(
            [
                "docker",
                "compose",
                "-f",
                "compose.yaml",
                "-f",
                "compose.full.yaml",
                "up",
                "-d",
                "api",
                "pipeline-exporter",
                "prometheus",
                "grafana",
            ]
        )
        execute(
            ["docker", "compose", "-f", "compose.yaml", "-f", "compose.full.yaml", "restart", "api"]
        )
        result = verify_serving()
        state.update(
            status="complete",
            complete=True,
            stage="complete",
            result=result,
            finished_at=datetime.now(UTC).isoformat(),
        )
        atomic_json(STATUS, state)
        print(json.dumps(state, indent=2))
    except Exception as error:
        state.update(status="failed", error=f"{type(error).__name__}: {error}")
        atomic_json(STATUS, state)
        raise


if __name__ == "__main__":
    main()
