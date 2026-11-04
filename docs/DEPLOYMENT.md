# Free hosted model demo

**Live:** [urbanlake-three.vercel.app](https://urbanlake-three.vercel.app)
([API documentation](https://urbanlake-three.vercel.app/docs)). The production
deployment became ready on November 4, 2026 and unauthenticated live HTTP checks
passed at **03:24:40 EST**. The exact release is local commit
`LOCAL_REVISION`; project identifiers are retained in
`deploy/vercel/project.json` so future releases reuse the same project.

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

The published release passed these unauthenticated HTTP checks. Its model version
is `LOCAL_MODEL_VERSION`; the historical November 1 prediction was **10,922.37** with
`cached=false`. The fixed snapshot reports all **23,008,706 requests**, 83 months,
100% completion, and its original capture time. Unsupported dates return 422.
Vercel's build log verified the restored 828,220-byte model and completed the
deployment. No paid resources or account-plan upgrades were created.

Live results and provider identifiers are saved under ignored
`artifacts/public-deployment-verification.json` and `artifacts/public-deployment.json`.
Visual browser verification was unavailable because the browser access host was
unavailable. The hosted/local JavaScript interaction checks and actual HTTP
model/prediction checks passed; no screenshot verification is claimed.
