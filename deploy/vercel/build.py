"""Restore the trusted, checksummed model before Vercel bundles the function."""

import gzip
import hashlib
import json
from pathlib import Path

base = Path(__file__).resolve().parent
manifest = json.loads((base / "assets/release.json").read_text(encoding="utf-8"))
model = gzip.decompress((base / "assets/full-model.joblib.gz").read_bytes())
if hashlib.sha256(model).hexdigest() != manifest["model_sha256"]:
    raise ValueError("model checksum differs from the verified release")
(base / "assets/full-model.joblib").write_bytes(model)
print(f"Verified inference model: {len(model):,} bytes")
