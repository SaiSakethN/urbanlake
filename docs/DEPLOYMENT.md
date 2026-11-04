# Free hosted UrbanLake application

**Live:** [urbanlake-three.vercel.app](https://urbanlake-three.vercel.app)
([API documentation](https://urbanlake-three.vercel.app/docs)). The production
deployment became ready on November 4, 2026 and unauthenticated live HTTP checks
passed at **03:24:40 EST** for the initial inference/progress release. The complete
application supersedes that interface. Its source revision and file hashes are
recorded by the release manifest and deployment evidence under `artifacts/`;
project identifiers stay in the ignored local `deploy/vercel/project.json` so
updates reuse the same project. The repository contains only an empty
`deploy/vercel/project.example.json` template.

The application opens at an analytics overview, with five functional views:

- **Overview:** actual full-dataset totals, monthly trends, borough/category rankings.
- **Explore:** month, borough and category filters; searchable, sortable category
  table with pagination; CSV export using the selected filters.
- **Forecast:** real model inference, source freshness, held-out evaluation and
  supported historical target date.
- **Data quality:** actual source anomalies alongside independent global count
  reconciliation, with the two meanings clearly separated.
- **Architecture:** the implemented pipeline, technologies and hosting boundaries.

Progress is a secondary page at `/progress`; the root URL serves the project app.
Private deployment identifiers and traceability records remain in ignored local
artifacts. Unauthenticated public application
checks passed at **10:01:48 EST on November 4, 2026**, including the root app,
metadata, total and filtered analytics, category selection, CSV export, project
evidence, model identity and actual prediction. The Queens/2026 query returned
830,304 requests and its 11 monthly CSV rows summed to the same count.
Analytics use all **23,008,706 requests**, represented by **62,061 aggregate facts**
across **83 months, 7 borough labels and 276 complaint categories**. A Gold-only
Spark export reconciles both fact totals and daily totals against the full audit.
The aggregate export is 297,134 bytes compressed and contains no request IDs,
addresses or coordinates. Current status counts reflect the captured observations;
they do not reconstruct historical backlog transitions. The partial latest source
month is marked explicitly in the chart.

UrbanLake's public release also serves the trained FastAPI model and API docs. Kafka,
Spark/Delta, Airflow, MLflow, Redis, and the raw request archive remain on the
local machine. The hosted API performs real model inference with Redis disabled.

The selected Vercel account uses the free Hobby plan. No paid database, storage,
domain, build upgrade, or scheduled task is needed. This personal portfolio demo
is subject to [Hobby usage limits](https://vercel.com/docs/plans/hobby), including
possible cold starts or temporary unavailability after a quota is reached.

## Release contents

`scripts/prepare_public_release.py` creates a new ignored directory under
`artifacts/`. It copies only the deployment entrypoint/configuration, five
application source files and both interfaces, the compressed trained model,
validated aggregate facts, normalized progress and project verification metadata.
It excludes raw requests, environments,
credentials, infrastructure state, the Git repository, and unrelated artifacts.
Release tests check these boundaries and reject incomplete audit/model/analytics
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
API docs, and frozen progress. The complete-app unit suite passed **109 tests**,
including filter validation, CSV formula escaping, aggregate reconciliation,
privacy boundaries and project evidence. The packaged app also passed actual
FastAPI requests using its real model and aggregate assets: all five app views,
23,008,706 total requests, a Queens/2026 selection of 830,304 requests, matching
11-row monthly CSV, and the real historical forecast. JavaScript syntax and HTML
element references passed checks. An additional Docker check was not executed
when automatic approval review encountered an account usage limit; it is not
claimed as completed for this app revision.

After deployment, verify `/health`, `/ready`, `/v1/model`, `/v1/progress`, `/docs`,
and an actual POST to `/v1/predictions/demand`. The returned model version must
match the trusted release. Keep the source cutoff visible: this model uses
complete data through October 31, 2026 and supports a historical November 1
forecast. It cannot forecast the current day from stale data.

The app additionally requires `/v1/analytics/metadata`, filtered `/v1/analytics`,
`/v1/analytics/export` and `/v1/project`. These are read-only queries over the
validated aggregate artifact; the browser never submits SQL or starts cloud jobs.
Reproduce the complete app's public checks with:

```powershell
.\.venv\Scripts\python.exe scripts/verify_public_app.py
```

The result is recorded in `artifacts/public-application-verification.json`.

The published release passed these unauthenticated HTTP checks. Its model checksum
matched the trusted training artifact; the historical November 1 prediction was **10,922.37** with
`cached=false`. The fixed snapshot reports all **23,008,706 requests**, 83 months,
100% completion, and its original capture time. Unsupported dates return 422.
Vercel's build log verified the restored 828,220-byte model and completed the
deployment. No paid resources or account-plan upgrades were created.

Live results and provider identifiers are saved under ignored
`artifacts/public-deployment-verification.json` and `artifacts/public-deployment.json`.
Visual browser verification was unavailable because the browser access host was
unavailable. The hosted/local JavaScript interaction checks and actual HTTP
model/prediction checks passed; no screenshot verification is claimed.
