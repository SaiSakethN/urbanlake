"""Public project evidence built from explicit aggregate fields only."""

import json
import os
from datetime import UTC, datetime
from pathlib import Path

AUDIT_FIELDS = (
    "status",
    "source_months",
    "archived_observations",
    "bronze_rows",
    "distinct_observation_ids",
    "distinct_source_keys",
    "silver_rows",
    "silver_distinct_source_keys",
    "gold_demand_requests",
    "gold_analytics_requests",
    "invalid_requests",
    "source_reference_count_matches",
    "daily_demand_days",
    "demand_export_days",
    "daily_demand_feature_rows",
    "excluded_latest_source_day",
    "scope_start",
    "scope_end_exclusive",
)
TRAINING_FIELDS = (
    "test_rf_mae",
    "test_rf_rmse",
    "test_seasonal_naive_mae",
    "validation_rf_mae",
    "validation_seasonal_naive_mae",
    "train_rows",
    "validation_rows",
    "test_rows",
    "test_selected_mae",
    "selected_model",
    "run_id",
    "as_of_date",
)


def validate(value: dict):
    audit = value["audit"]
    expected = audit["archived_observations"]
    if value["status"] != "complete" or audit["status"] != "passed":
        raise ValueError("project evidence is incomplete")
    if not isinstance(expected, int) or expected <= 0:
        raise ValueError("project evidence has no source records")
    for field in (
        "bronze_rows",
        "distinct_observation_ids",
        "distinct_source_keys",
        "silver_rows",
        "silver_distinct_source_keys",
        "gold_demand_requests",
        "gold_analytics_requests",
    ):
        if audit[field] != expected:
            raise ValueError("project evidence counts do not reconcile")
    if not audit["source_reference_count_matches"] or audit["invalid_requests"]:
        raise ValueError("project source/date checks did not pass")
    return value


def build(workspace: Path = Path("."), *, captured_at: str | None = None, mode="local"):
    audit = json.loads((workspace / "artifacts/full-verification.json").read_text())
    training_path = workspace / "artifacts/full-model.metrics.json"
    training = json.loads(training_path.read_text()) if training_path.exists() else {}
    result = {
        "schema_version": 1,
        "status": "complete",
        "mode": mode,
        "captured_at": captured_at or datetime.now(UTC).isoformat(),
        "audit": {field: audit[field] for field in AUDIT_FIELDS if field in audit},
        "model_training": {
            field: training[field] for field in TRAINING_FIELDS if field in training
        },
        "deployment": {
            "hosted": ["FastAPI", "Model inference", "Aggregate analytics"],
            "local": [
                "Kafka",
                "Spark/Delta",
                "Airflow",
                "MLflow",
                "Raw request archive",
                "Redis",
            ],
        },
    }
    return validate(result)


def overview(workspace: Path = Path(".")):
    if "PROJECT_OVERVIEW_PATH" in os.environ:
        path = os.environ["PROJECT_OVERVIEW_PATH"]
        if not path:
            raise ValueError("project evidence path is empty")
        return validate(json.loads(Path(path).read_text(encoding="utf-8")))
    return build(workspace)
