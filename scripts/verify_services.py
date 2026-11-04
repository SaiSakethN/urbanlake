"""Assert live API/Redis/MLflow/Prometheus/Grafana integration (no mocked services)."""

import json
from pathlib import Path

import httpx
import redis

with httpx.Client(timeout=10) as client:
    assert client.get("http://localhost:5000/health").status_code == 200
    training = json.loads(Path("artifacts/model.metrics.json").read_text())
    tracked = client.get(
        "http://localhost:5000/api/2.0/mlflow/runs/get",
        params={
            "run_id": training["run_id"],
        },
    )
    assert tracked.status_code == 200, tracked.text
    run = tracked.json()["run"]
    assert run["info"]["status"] == "FINISHED", run["info"]
    tracked_metrics = {metric["key"]: metric["value"] for metric in run["data"]["metrics"]}
    assert abs(tracked_metrics["test_selected_mae"] - training["test_selected_mae"]) < 1e-8
    assert client.get("http://localhost:8000/health").status_code == 200
    ready = client.get("http://localhost:8000/ready")
    assert ready.status_code == 200, ready.text
    assert ready.json()["cache"] == "up", ready.text
    model = client.get("http://localhost:8000/v1/model").json()
    payload = {"schema_version": 1, "target_date": model["supported_target_date"]}
    first = client.post("http://localhost:8000/v1/predictions/demand", json=payload)
    assert first.status_code == 200, first.text
    second = client.post("http://localhost:8000/v1/predictions/demand", json=payload)
    assert second.status_code == 200 and second.json()["cached"], second.text
    assert first.json()["predicted_requests"] == second.json()["predicted_requests"]
    cache = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
    key = f"urbanlake:demand:v1:{model['model_version']}:{payload['target_date']}"
    assert json.loads(cache.get(key))["predicted_requests"] == first.json()["predicted_requests"]
    invalid = client.post(
        "http://localhost:8000/v1/predictions/demand", json={"target_date": "invalid"}
    )
    assert invalid.status_code == 422 and "error" in invalid.json()
    targets = client.get("http://localhost:9090/api/v1/targets").json()["data"]["activeTargets"]
    assert len(targets) == 2 and all(t["health"] == "up" for t in targets), targets
    pipeline = client.get(
        "http://localhost:9090/api/v1/query",
        params={
            "query": "urbanlake_pipeline_bronze_observations",
        },
    ).json()
    assert float(pipeline["data"]["result"][0]["value"][1]) == 320, pipeline
    assert client.get("http://localhost:3000/api/health").json()["database"] == "ok"
    dashboard = client.get("http://localhost:3000/api/dashboards/uid/urbanlake-operations").json()
    assert dashboard["dashboard"]["title"] == "UrbanLake Operations", dashboard
    assert len(dashboard["dashboard"]["panels"]) == 8
    report = {
        "status": "passed",
        "api_prediction": second.json(),
        "model_metadata": model,
        "redis": "actual cached response verified",
        "mlflow": f"FINISHED run {training['run_id']}; evaluation metric matches local report",
        "prometheus_targets": [{"job": t["labels"]["job"], "health": t["health"]} for t in targets],
        "grafana": "database healthy; eight-panel dashboard provisioned",
    }
    Path("artifacts/services-result.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
