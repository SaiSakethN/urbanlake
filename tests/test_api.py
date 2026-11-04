import joblib
import redis
from fastapi.testclient import TestClient

from urbanlake.api import app


class Cache:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def setex(self, key, ttl, value):
        self.data[key] = value

    def ping(self):
        return True

    def close(self):
        pass


def test_health_and_missing_model(monkeypatch, tmp_path):
    monkeypatch.setenv("MODEL_PATH", str(tmp_path / "missing.joblib"))
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        assert client.get("/ready").status_code == 503
        assert client.post("/v1/predictions/demand", json={"target_date": "bad"}).status_code == 422
        assert (
            client.post("/v1/predictions/demand", json={"target_date": "2026-01-15"}).status_code
            == 503
        )


def test_prediction_cache_and_date_validation(monkeypatch, tmp_path):
    file = tmp_path / "model.joblib"
    joblib.dump(
        {
            "as_of_date": "2026-01-14",
            "history": list(range(14)),
            "selected_model": "seasonal_naive",
        },
        file,
    )
    monkeypatch.setenv("MODEL_PATH", str(file))
    with TestClient(app) as client:
        app.state.cache = Cache()
        payload = {"target_date": "2026-01-15"}
        first = client.post("/v1/predictions/demand", json=payload)
        assert first.status_code == 200
        assert first.json()["predicted_requests"] == 7
        assert first.json()["cached"] is False
        second = client.post("/v1/predictions/demand", json=payload)
        assert second.json()["cached"] is True
        invalid = client.post("/v1/predictions/demand", json={"target_date": "2026-01-20"})
        assert invalid.status_code == 422
        assert "error" in invalid.json()
        assert (
            client.post("/v1/predictions/demand", json={**payload, "schema_version": 2}).status_code
            == 422
        )
        assert "urbanlake_http_requests_total" in client.get("/metrics").text


def test_cache_outage_does_not_prevent_forecast(monkeypatch, tmp_path):
    class Offline(Cache):
        def get(self, key):
            raise redis.ConnectionError("offline")

        def setex(self, *args):
            raise redis.ConnectionError("offline")

    file = tmp_path / "model.joblib"
    joblib.dump(
        {
            "as_of_date": "2026-01-14",
            "history": list(range(14)),
            "selected_model": "seasonal_naive",
        },
        file,
    )
    monkeypatch.setenv("MODEL_PATH", str(file))
    with TestClient(app) as client:
        app.state.cache = Offline()
        assert (
            client.post("/v1/predictions/demand", json={"target_date": "2026-01-15"}).status_code
            == 200
        )


def test_disabled_redis_never_constructs_or_connects_a_cache(monkeypatch, tmp_path):
    file = tmp_path / "model.joblib"
    joblib.dump(
        {
            "as_of_date": "2026-01-14",
            "history": list(range(14)),
            "selected_model": "seasonal_naive",
        },
        file,
    )
    monkeypatch.setenv("MODEL_PATH", str(file))
    monkeypatch.setenv("REDIS_ENABLED", "false")

    def forbidden(*args, **kwargs):
        raise AssertionError("disabled Redis must not construct a client")

    monkeypatch.setattr(redis.Redis, "from_url", forbidden)
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        ready = client.get("/ready")
        assert ready.status_code == 200 and ready.json()["cache"] == "disabled"
        for _ in range(2):
            forecast = client.post("/v1/predictions/demand", json={"target_date": "2026-01-15"})
            assert forecast.status_code == 200
            assert forecast.json()["predicted_requests"] == 7
            assert forecast.json()["cached"] is False
