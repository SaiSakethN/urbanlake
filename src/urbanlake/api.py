"""Versioned next-day forecasts; model artifact is trusted local code (joblib)."""

import hashlib
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

import joblib
import redis
from fastapi import FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, ConfigDict

from urbanlake.ml import predict

log = logging.getLogger("urbanlake.api")
REQUESTS = Counter("urbanlake_http_requests_total", "HTTP requests", ["route", "status"])
LATENCY = Histogram("urbanlake_http_seconds", "HTTP request duration", ["route"])
CACHE = Counter("urbanlake_prediction_cache_total", "Prediction cache outcomes", ["outcome"])


class PredictionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    target_date: date


class PredictionResponse(BaseModel):
    schema_version: Literal[1] = 1
    target_date: date
    predicted_requests: float
    model_version: str
    model_type: str
    as_of_date: date
    cached: bool


@asynccontextmanager
async def lifespan(application):
    path = Path(os.getenv("MODEL_PATH", "artifacts/model.joblib"))
    application.state.bundle = None
    application.state.version = None
    if path.exists():
        try:
            application.state.version = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
            application.state.bundle = joblib.load(path)
        except Exception:
            log.exception("Model artifact could not be loaded")
    application.state.cache = redis.Redis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        socket_connect_timeout=0.25,
        socket_timeout=0.25,
        decode_responses=True,
    )
    yield
    application.state.cache.close()


app = FastAPI(title="UrbanLake Demand API", version="1.0.0", lifespan=lifespan)


@app.get("/progress", response_class=HTMLResponse, include_in_schema=False)
def progress_page():
    return HTMLResponse(
        (Path(__file__).parent / "static/progress.html").read_text(encoding="utf-8"),
        headers={"Cache-Control": "no-store"},
    )


@app.get("/v1/progress")
def progress_status():
    from urbanlake.progress import snapshot

    try:
        return JSONResponse(snapshot(), headers={"Cache-Control": "no-store"})
    except (OSError, ValueError, KeyError) as error:
        raise HTTPException(
            503, "progress snapshot temporarily unavailable; retry shortly"
        ) from error


@app.exception_handler(HTTPException)
async def http_error(request, exc):
    return JSONResponse(status_code=exc.status_code, content={"error": {"message": exc.detail}})


@app.exception_handler(RequestValidationError)
async def validation_error(request, exc):
    return JSONResponse(
        status_code=422,
        content={
            "error": {"message": "invalid request", "details": jsonable_encoder(exc.errors())}
        },
    )


@app.middleware("http")
async def metrics(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    # Route templates avoid unbounded label cardinality from arbitrary request paths.
    route = request.scope.get("route")
    label = route.path if route else "unmatched"
    REQUESTS.labels(label, str(response.status_code)).inc()
    LATENCY.labels(label).observe(time.perf_counter() - start)
    return response


@app.get("/health")
def health():
    return {"status": "ok", "service": "urbanlake", "schema_version": 1}


@app.get("/ready")
def ready(request: Request):
    if request.app.state.bundle is None:
        raise HTTPException(503, "model unavailable; run training and restart the API")
    try:
        request.app.state.cache.ping()
        cache = "up"
    except redis.RedisError:
        cache = "unavailable"
    return {
        "status": "ready",
        "model_version": request.app.state.version,
        "cache": cache,
        **model_metadata(request.app.state.bundle),
    }


def model_metadata(bundle):
    as_of = date.fromisoformat(bundle["as_of_date"])
    age = (datetime.now(ZoneInfo("America/New_York")).date() - as_of).days
    return {
        "as_of_date": str(as_of),
        "supported_target_date": str(as_of + timedelta(days=1)),
        "data_age_days": age,
        "source_stale": age > 1,
    }


@app.get("/v1/model")
def model_info(request: Request):
    bundle = request.app.state.bundle
    if bundle is None:
        raise HTTPException(503, "model unavailable; run training and restart the API")
    return {
        "schema_version": 1,
        "model_version": request.app.state.version,
        "model_type": bundle["selected_model"],
        "mlflow_run_id": bundle.get("run_id"),
        "training_data_sha256": bundle.get("dataset_sha256"),
        "evaluation": {
            "test_days": bundle.get("metrics", {}).get("test_rows"),
            "mean_absolute_error_requests": bundle.get("metrics", {}).get("test_selected_mae"),
            "seasonal_baseline_mean_absolute_error_requests": bundle.get("metrics", {}).get(
                "test_seasonal_naive_mae"
            ),
        },
        **model_metadata(bundle),
    }


@app.get("/metrics", include_in_schema=False)
def prometheus_metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/v1/predictions/demand", response_model=PredictionResponse)
def forecast(payload: PredictionRequest, request: Request):
    bundle = request.app.state.bundle
    if bundle is None:
        raise HTTPException(503, "model unavailable; run training and restart the API")
    version = request.app.state.version
    key = f"urbanlake:demand:v1:{version}:{payload.target_date}"
    try:
        cached = request.app.state.cache.get(key)
        if cached:
            CACHE.labels("hit").inc()
            return {**json.loads(cached), "cached": True}
    except redis.RedisError:
        CACHE.labels("error").inc()
    CACHE.labels("miss").inc()
    try:
        estimate = predict(bundle, payload.target_date)
    except ValueError as error:
        raise HTTPException(422, str(error)) from error
    response = {
        "schema_version": 1,
        "target_date": str(payload.target_date),
        "predicted_requests": round(estimate, 2),
        "model_version": version,
        "model_type": bundle["selected_model"],
        "as_of_date": bundle["as_of_date"],
        "cached": False,
    }
    try:
        request.app.state.cache.setex(key, 3600, json.dumps(response))
    except redis.RedisError:
        CACHE.labels("error").inc()
    return response
