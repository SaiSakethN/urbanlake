"""One-step-ahead demand forecasting with chronological evaluation.

No status/closure fields are model inputs. Daily counts are *current reconstructed*
history, not point-in-time snapshots; historical API corrections remain a limitation.
"""

import hashlib
import json
import os
from datetime import timedelta
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

FEATURES = ["lag1", "lag7", "lag14", "mean7", "weekday", "month"]


def read_daily(path: str) -> pd.DataFrame:
    files = sorted(Path(path).glob("*.csv")) if Path(path).is_dir() else [Path(path)]
    if not files:
        raise ValueError("no daily demand CSV files; run Spark first")
    frame = pd.concat([pd.read_csv(file) for file in files], ignore_index=True)
    frame["day"] = pd.to_datetime(frame["day"], errors="raise").dt.normalize()
    frame["requests"] = pd.to_numeric(frame["requests"], errors="raise")
    frame = frame.sort_values("day").reset_index(drop=True)
    if frame.day.duplicated().any() or frame.requests.isna().any() or (frame.requests < 0).any():
        raise ValueError("daily counts must be unique, finite and nonnegative")
    if not np.isfinite(frame.requests).all() or not (frame.requests % 1 == 0).all():
        raise ValueError("daily counts must be finite integers")
    if len(frame) < 60 or not frame.day.diff().dropna().eq(pd.Timedelta(days=1)).all():
        raise ValueError("need at least 60 contiguous complete days")
    # Never train on today's partially accumulated day (NYC calendar).
    today = pd.Timestamp.now(tz="America/New_York").date()
    if frame.day.max().date() >= today:
        raise ValueError("daily demand must exclude the current NYC day")
    return frame


def features(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame[["day", "requests"]].copy()
    for lag in (1, 7, 14):
        result[f"lag{lag}"] = result.requests.shift(lag)
    result["mean7"] = result.requests.shift(1).rolling(7).mean()
    result["weekday"] = result.day.dt.dayofweek
    result["month"] = result.day.dt.month
    return result.dropna().reset_index(drop=True)


def next_features(history: list[float], target_date) -> pd.DataFrame:
    if len(history) < 14:
        raise ValueError("14 complete historical days required")
    row = [
        history[-1],
        history[-7],
        history[-14],
        float(np.mean(history[-7:])),
        target_date.weekday(),
        target_date.month,
    ]
    return pd.DataFrame([row], columns=FEATURES)


def predict(bundle: dict, target_date) -> float:
    expected = pd.Timestamp(bundle["as_of_date"]).date() + timedelta(days=1)
    if target_date != expected:
        raise ValueError(f"this model snapshot supports only target_date={expected}")
    if bundle["selected_model"] == "seasonal_naive":
        return float(bundle["history"][-7])
    return max(
        0.0, float(bundle["model"].predict(next_features(bundle["history"], target_date))[0])
    )


def train(args):
    import mlflow
    import mlflow.sklearn

    daily = read_daily(args.input)
    table = features(daily)
    cut = int(len(table) * 0.8)
    development, test = table.iloc[:cut], table.iloc[cut:]
    # Separate validation tail chooses the deployment candidate; untouched test reports quality.
    validation_cut = int(len(development) * 0.8)
    fit, validation = development.iloc[:validation_cut], development.iloc[validation_cut:]
    model = RandomForestRegressor(
        n_estimators=100, max_depth=6, min_samples_leaf=3, random_state=42, n_jobs=2
    )
    model.fit(fit[FEATURES], fit.requests)
    validation_model_mae = mean_absolute_error(
        validation.requests, model.predict(validation[FEATURES])
    )
    validation_baseline_mae = mean_absolute_error(validation.requests, validation.lag7)
    selected = (
        "random_forest" if validation_model_mae < validation_baseline_mae else "seasonal_naive"
    )
    model.fit(development[FEATURES], development.requests)
    estimates = model.predict(test[FEATURES])
    metrics = {
        "test_rf_mae": float(mean_absolute_error(test.requests, estimates)),
        "test_rf_rmse": float(mean_squared_error(test.requests, estimates) ** 0.5),
        "test_seasonal_naive_mae": float(mean_absolute_error(test.requests, test.lag7)),
        "validation_rf_mae": float(validation_model_mae),
        "validation_seasonal_naive_mae": float(validation_baseline_mae),
        "train_rows": len(development),
        "validation_rows": len(validation),
        "test_rows": len(test),
    }
    metrics["test_selected_mae"] = metrics[
        "test_rf_mae" if selected == "random_forest" else "test_seasonal_naive_mae"
    ]
    dataset_hash = hashlib.sha256(
        daily[["day", "requests"]].to_csv(index=False).encode()
    ).hexdigest()
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path("artifacts").mkdir(exist_ok=True)
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "sqlite:///artifacts/mlflow.db"))
    mlflow.set_experiment("urbanlake-daily-demand")
    with mlflow.start_run() as run:
        mlflow.log_params(
            {
                "target": "citywide_next_day_request_count",
                "dataset_sha256": dataset_hash,
                "split": "chronological_64pct_development_16pct_validation_20pct_test",
                "evaluation": "rolling_one_day_ahead_observed_lags",
                "selected_model": selected,
                "first_day": str(daily.day.min().date()),
                "last_day": str(daily.day.max().date()),
                "features": ",".join(FEATURES),
                "random_state": 42,
            }
        )
        mlflow.log_metrics(metrics)
        # Refit the already selected candidate on all available complete days for serving.
        model.fit(table[FEATURES], table.requests)
        bundle = {
            "schema_version": 1,
            "model": model,
            "selected_model": selected,
            "as_of_date": str(daily.day.max().date()),
            "history": daily.requests.tail(14).tolist(),
            "run_id": run.info.run_id,
            "dataset_sha256": dataset_hash,
            "metrics": metrics,
        }
        temporary = str(args.output) + ".tmp"
        joblib.dump(bundle, temporary)
        mlflow.sklearn.log_model(
            model, name="candidate_random_forest", input_example=table[FEATURES].head(2)
        )
        mlflow.log_dict({k: v for k, v in bundle.items() if k != "model"}, "model_card.json")
        # Do not publish a local serving model if experiment tracking failed.
        Path(temporary).replace(args.output)
        report = {
            **metrics,
            "selected_model": selected,
            "run_id": run.info.run_id,
            "as_of_date": bundle["as_of_date"],
        }
        Path(args.output).with_suffix(".metrics.json").write_text(json.dumps(report, indent=2))
        print(json.dumps(report, indent=2))
