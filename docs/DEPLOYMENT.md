# Free hosted model demo

UrbanLake's public release serves the trained FastAPI model, API documentation,
and a captured dashboard of the independently verified full-data run. Kafka,
Spark/Delta, Airflow, MLflow, Redis, and the raw request archive remain on the
local machine. The hosted API performs real model inference with Redis disabled.

The selected Vercel account uses the free Hobby plan. No paid database, storage,
domain, build upgrade, or scheduled task is needed. This personal portfolio demo
is subject to [Hobby usage limits](https://vercel.com/docs/plans/hobby), including
possible cold starts or temporary unavailability after a quota is reached.

## Release contents

`scripts/prepare_public_release.py` creates a new ignored directory under
`artifacts/`. It copies only the deployment entrypoint/configuration, five
application source files, the compressed trained model, normalized progress,
and aggregate verification metadata. It excludes raw requests, environments,
credentials, infrastructure state, the Git repository, and unrelated artifacts.
Eight release tests check these boundaries and reject incomplete audit/model
evidence or a changed model identity.

The full trained model is 828,220 bytes. Lossless gzip reduces its upload size;
the build restores it and verifies its original SHA-256 before serving. The API
uses the original pinned scikit-learn, NumPy, SciPy, and pandas versions. Its
inference dependencies are separate from the full platform's dependencies.

```powershell
.\.venv\Scripts\python.exe scripts/prepare_public_release.py
```

The release manifest records its source commit, file hashes, capture time and
model identity. Generate a new release for later model updates. The hosted
dashboard deliberately preserves its capture time and identifies processing as
a verified local run; it does not claim a live connection to the laptop or poll
the full ingestion pipeline.

## Verification

Before upload, the exact release passed actual FastAPI requests in Linux with
the release mounted read-only and networking disabled. Checks covered model
identity, real prediction parity, disabled Redis, unsupported-date rejection,
API docs, and frozen progress. The full local suite passed 77 tests, including
the release isolation checks; lint checks passed. JavaScript checks covered the
hosted forecast interaction and preservation of local progress polling.

After deployment, verify `/health`, `/ready`, `/v1/model`, `/v1/progress`, `/docs`,
and an actual POST to `/v1/predictions/demand`. The returned model version must
match the trusted release. Keep the source cutoff visible: this model uses
complete data through October 31, 2026 and supports a historical November 1
forecast. It cannot forecast the current day from stale data.
