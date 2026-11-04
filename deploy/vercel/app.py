"""Public inference and a captured audit; the data platform remains local."""

import os
import sys
from pathlib import Path

from fastapi.responses import RedirectResponse

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE / "src"))
os.environ.setdefault("MODEL_PATH", str(BASE / "assets/full-model.joblib"))
os.environ.setdefault("PROGRESS_SNAPSHOT_PATH", str(BASE / "assets/progress.json"))
os.environ.setdefault("REDIS_ENABLED", "false")

from urbanlake.api import app  # noqa: E402


@app.get("/", include_in_schema=False)
def home():
    return RedirectResponse("/progress")
