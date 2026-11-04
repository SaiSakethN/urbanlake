# Verification ledger

Status is scoped to the evidence below; this is a local portfolio build, not a
production-readiness certification.

## Initial build checks (historical notes)

- Existing virtual environment: Python 3.12.10.
- Docker engine 29.5.2 and Compose 5.1.3 responded successfully.
- `pip install -e '.[dev]'`: completed after sandbox network approval.
- `pip check`: no broken requirements.
- `pytest -q`: **12 passed**, two dependency deprecation warnings.
  Tests cover bounded ingestion/retries, replay IDs, temporal features, request
  validation, cache hits, and graceful Redis outage behavior (mocked cache).
- Source freshness probe: max `created_date=2026-11-01T02:06:29.000`.
  Requested July 6–November 3 aggregate window rejected for missing dates.
- Official API: **120 daily aggregate observations** fetched, July 4–October 31,
  2026. Source's latest possibly partial day excluded. No full dataset download.
- Spark image: Python 3.12.10, Java 17, PySpark 4.0.1, Delta 4.0.0 installed;
  actual Spark execution pending.
- Public `apache/kafka:4.0.1` image download failed three times with registry EOF.
  Optional `compose.local-cache.yaml` uses cached Kafka **4.3.2**, verified by
  `kafka-topics.sh --version`. Broker passed healthcheck and topic init started.
  This cache-specific override is not a portable public image dependency.

## Final component ledger

| Component | Configured | Implemented | Verified scope |
|---|---|---|---|
| Socrata bounded ingestion | yes | yes | unit tests + live 200-row sample / 120 aggregate days |
| Kafka publication / consumption | yes | yes | acknowledgments, Spark consumption, full offset rewind |
| Spark/Delta medallion | yes | yes | live data, synthetic correction, replay, quarantine |
| Gold SQL / ML features | yes | yes | totals/keys + all 106 Gold/Python feature rows match |
| Forecasting / MLflow / PostgreSQL | yes | yes | real training, held-out metrics, FINISHED tracked run |
| FastAPI / Redis | yes | yes | endpoint tests + actual model / Redis cache hit |
| Airflow | yes | yes | DB + scheduler startup, three paused DAGs, manual smoke success |
| Prometheus / Grafana | yes | yes | two scraped targets, four parsed rules, eight provisioned panels |
| GitHub Actions CI | yes | yes | constituent checks ran locally; remote CI not run |
| External CD / GitHub publishing | no | intentionally deferred | no push/deployment authorized |

## Additional runtime evidence

- Kafka acknowledged **320** archived observations. Initial live-source Spark run
  produced 320 Bronze observations, 200 Silver keys, 120 daily demand rows, and
  39 Gold request groups. Independent Delta reads asserted unique IDs/keys and a
  Gold request sum of 200.
- A separate synthetic Kafka topic/lake contained 67 unique observations, one
  latest request and one observed content change. Rewinding all Kafka offsets with
  a new checkpoint against the same lake left all counts unchanged.
- Registry download failures were resolved without depending on the cached
  third-party image: final Kafka **4.0.1** is built from the official Apache release
  archive with a pinned SHA-512 checksum. It passed broker health and publication.
  Redis **7.0.15** was built from Debian's security package. PostgreSQL **15.18**
  runs from the digest-pinned cached official image; separate Airflow DB created.
- MLflow runtime initially failed with SQLAlchemy 2.1.7 despite `pip check` passing.
  Pinning SQLAlchemy 2.0.43 fixed startup. NumPy 2.2.6 and SciPy 1.16.1 were also
  pinned; local `pip check` and container `pip check` passed.
- Forecast run `LOCAL_RUN_ID`: 84 development rows (including
  17 validation rows after candidate selection), 22 test days. Random forest
  validation MAE 595.2601 versus seasonal naive 764.4706. Test MAE **744.7659**,
  RMSE **950.4587**, seasonal-naive test MAE **889.7727** requests/day.
  Random forest selected on validation; refit on all feature rows for serving.
- Real service check `scripts/verify_services.py` passed: API health/readiness,
  model metadata, prediction, actual Redis cached object and cache hit, structured
  422 response, MLflow health, **two healthy Prometheus targets**, and Grafana
  database health plus the **eight-panel provisioned dashboard**.
  Example historical forecast for November 1: **10,212.99** requests; this is a
  model output, not an observed count or forecast for the current day.
- `promtool check config`: valid configuration, one alert rule file, **four rules**.
  Prometheus is fetched from the official Quay mirror after Docker Hub EOF errors.
- Airflow database migration passed. Dedicated virtual environment initially
  lacked psycopg2; adding it under Airflow's official constraints fixed startup.
  All **three DAGs** listed; import-error listing returned no errors.
  Scheduled recent/wider DAGs remain unexecuted and paused by design.
- Latest local unit run: **17 passed**, one non-failing Starlette/AnyIO
  deprecation warning. Ruff lint and format checks passed before the last test
  additions; final checks will be repeated with the final source tree.

The **manual Airflow smoke DAG passed**, including all three tasks: saved-sample
publication, Spark medallion processing, and MLflow-backed training. It used only
the already archived observations and made no source API download. Training run
`LOCAL_RUN_ID` reproduced the same evaluation metrics. Spark
also wrote the new Gold lag-feature table. Airflow `dags test` reports a synthetic
run duration based on logical execution time; it is not used as build elapsed time.

The **expanded fixture passed**: 67 unique Bronze observations, one current Silver
request, one observed content difference, 65 demand days, 51 Gold feature rows,
and one quarantined malformed message. All assertions passed again after a full
Kafka offset rewind against the same tables.

The **independent live-lake verification passed**, including exact comparison of
all 106 Spark Gold feature rows with Python training features. Unique Bronze and
Silver keys, 120 demand days, 39 request groups, and total request volume 200 were
also asserted. Synthetic test metrics never overwrite the live lake's metrics.

Airflow standalone was started on loopback port **8081**, because an unrelated
Airflow project already owned 8080. Its DB check and isolated environment's
`pip check` passed. The scheduler listed all three DAGs explicitly paused. No
recent/wider scheduled API workflow was enabled or run. API was restarted after
the smoke DAG trained its new artifact, and real service verification passed again,
including an MLflow REST read of the FINISHED run and metric equality with the
local evaluation report.

## Failures and fixes

- Sandbox denied initial network/Docker access; explicitly approved execution
  succeeded. No host Java/Spark installation was made.
- Docker Hub image-layer transfers returned EOF repeatedly. Final portable
  Apache-binary Kafka and Debian Redis builds, digest-pinned official PostgreSQL,
  official Quay Prometheus, and official Grafana image all ran successfully.
- Kafka's first custom build used an unwritable non-root log directory. Moving
  logs into a Kafka-owned directory fixed broker startup.
- Apache's checksum file groups hexadecimal words across lines. The first parser
  rejected it; the final downloader pins the exact official 128-digit SHA-512 and
  verified the archive before extraction. Verification was not bypassed.
- MLflow / SQLAlchemy and Airflow / psycopg2 runtime problems are documented above.
- Windows test temp-directory ACLs conflicted between sandboxed and approved
  executions. Tests now create a unique workspace temp directory for every run,
  preserving prior directories instead of deleting/reusing them. The final rerun
  passed **17 tests** using `pytest -q -p no:cacheprovider`; lint/format checks passed.
  One non-failing Starlette/AnyIO deprecation warning remains.
- Non-failing runtime warnings include missing optional Graphviz for Airflow
  graph rendering, missing Git executable for MLflow's automatic Git tag, integer
  feature schema guidance, native Hadoop fallback, and global windows on the small
  citywide daily series. None is represented as a failed test or silently hidden.

## Scope and production gaps

This verifies a bounded local vertical slice. It does **not** verify 23-million-row
scale, real multi-node distribution, broker failover, mid-commit crash recovery,
backup restoration, all historical source changes, source deletions, cloud storage,
public API security, model drift, fired alert delivery, visual dashboard rendering,
or GitHub-hosted CI/CD. Historical batch requests are implemented as bounded date
windows; a full raw historical backfill was not downloaded or run.

Silver/Gold currently rebuild from Bronze and publish separate transactions plus
a CSV feature-input export. One writer is required. This is recoverable by replay,
but not an atomic multi-table publication or a full-scale incremental lakehouse.
Successful source-window manifests are local artifacts rather than a durable
lake-wide completeness catalog. Sample analytics cannot establish citywide backlog.

Seven-day unresolved classification remains deferred for defensible label reasons.
The forecast uses 120 reconstructed aggregate days, not archived historical
point-in-time data; only 22 one-step-ahead test days were evaluated. No confidence
intervals, extensive tuning, drift monitor or production-quality guarantee exists.
The source was stale at inspection; the model supports a historical November 1
forecast and explicitly advertises that limitation.

## Next production milestones

1. Establish and monitor source availability/completeness and persist ingestion
   coverage/manifests in Delta; collect immutable point-in-time snapshots over time.
2. Replace full derived rebuilds with affected-key MERGEs and affected-partition
   Gold updates; define coordinated publication and validate concurrency/scale.
3. Move Delta/checkpoints/artifacts to durable object storage; add replicated Kafka,
   secured identities/TLS, managed secrets, backups and recovery drills.
4. Expand chronological model evaluation across seasons and rolling origins;
   implement registry/promotion controls, monitored model refresh and drift alerts.
5. Exercise bounded recent/wider scheduled DAGs under an explicitly accepted data
   budget; verify retry/crash paths, retention gaps, and actual alert notifications.
6. Authorize GitHub publishing, run the configured CI on a clean runner, and add
   an approved deployment workflow with protected environments and rollback tests.

No secrets, virtual environment, raw source data, model binaries, logs, or generated
runtime artifacts are intended for Git. Git commit IDs and final elapsed time are
recorded in the concluding build audit.

## Final handoff

Implementation/audit checkpoint: **01:28:55 UTC on November 4, 2026**, corresponding
to **8:28:55 p.m. Eastern on November 3**, approximately **92m50s** after the measured
start. This is before the 120-minute cutoff. Final tests repeated at handoff:
**17 passed**, one dependency deprecation warning; Ruff lint and format checks clean.

Created implementation commits: `LOCAL_REVISION` and `LOCAL_REVISION`; documentation is committed
separately. Original history is preserved. The full Git file list was inspected:
no raw observations, models, environments, credentials or runtime logs were tracked.
No GitHub push, external deployment, credential/account access or full backfill ran.

Local evidence remains under ignored `artifacts/`: `airflow-smoke.log`,
`integration.log`, `integration-result.json`, `lake-verification.log`,
`services-result.json`, `pipeline_metrics.json`, and `model.metrics.json`.
Reproduce checks with the commands in `README.md`; these generated artifacts are
deliberately excluded from version control.
