# Full-data extension

The initial 120-minute build was completed and audited separately. On November 3,
2026 the user then explicitly authorized processing the complete dataset. This
document records that new full-data phase; the earlier sample audit is preserved.

## Current full-run status

The authorized full-data project **completed at 2026-11-04 07:59:08 UTC**
(**02:59:08 EST on November 4**). All **83 source months** are downloaded and
finalized, covering **23,008,706 archived observations**. The independent global
audit passed, the full-history model was trained and tracked, and the locally
served model passed identity, Redis-cache, and monitoring checks.

| Gate | Current evidence |
|---|---|
| Source download | Complete: 83/83 months, 23,008,706 records |
| Kafka / monthly lakehouse completion | Complete: 83/83 months finalized |
| Independent global lake audit | Passed: archive, distinct Bronze/Silver keys, and Gold totals all 23,008,706 |
| Full-history model evaluation / MLflow | Passed: 497 held-out days, FINISHED MLflow run |
| Full-model API / Redis / monitoring verification | Passed: promoted model identity, actual cached prediction, full-record metrics |
| Restored local services | Passed at 08:00:54 UTC: API, MLflow, Prometheus, Grafana, Airflow health |
| Local unit suite | 64 passed; Ruff lint and format checks passed for 43 files |

`artifacts/full-project-status.json` now records `status=complete`, and the live
progress page reports 100%. The earlier failed 23,008,705-record audit is preserved
in the boundary-recovery backup; the current `artifacts/full-verification.json`
records `status=passed`. See the precision incident below.

## Measured source scope

Live API probe at 2026-11-04 01:37 UTC:

- Dataset: official NYC 311 `erm2-nwe9` (2020 to present).
- Records: **23,008,706**.
- Earliest creation: **2020-01-01T00:00:00.000** NYC local time.
- Latest creation: **2026-11-01T02:06:29.000** NYC local time.
- Immutable plan range: `[2020-01-01, 2026-11-01T02:06:29.000001)`.
- Monthly source partitions: **83**, including the partial final source month/day.
- 100-row size probe: approximately 1,030 raw JSON bytes/row, **23.69 GB** raw.
- Available disk at probe: **146.34 GB**; runtime reserve guard: **30 GiB**.
- First three pages: **15,000 records in 7.22 seconds**, 4.67 MB compressed.

The throughput pilot is not a full-run ETA guarantee. Source throttling, historical
record size, Kafka delivery, Delta jobs and retries affect total runtime. A
multi-hour full-data run was required. Completion is backed by the checks listed
above rather than extrapolated from the pilot.

## Full-data path

```text
Bounded monthly API pages
  -> atomic gzip observation archive + checksummed source manifest
  -> Kafka (zstd, acknowledgments, per-page publication state)
  -> Spark partition-pruned Bronze MERGE
  -> completed-month coverage gate
  -> affected-partition Silver MERGE and Gold SQL refresh
  -> small daily-series feature export
  -> full-history model evaluation and serving refresh
```

This preserves Kafka in the actual processing path. The archive enables download
resume and replay; reading archives directly into Spark is an explicit diagnostic
fallback, not the full-run coordinator's data path.

## Resume and completion semantics

The downloader validates strictly increasing `(created_date, unique_key)` cursors,
preserving observed cursor precision. Query bounds are rounded up to the source's
millisecond timestamp grid because SoQL truncates sub-millisecond literals. This
preserves the logical half-open plan range; the immutable plan is not widened.
The downloader commits compressed pages atomically,
records checksums/counts/cursors, and marks a month complete only after pagination
exhaustion and matching before/after source counts. A partial month is never
reported as complete. Actual per-page fetch times are preserved alongside stable
monthly observation identity. A disk reserve or failure stops progress visibly.

The relay commits page state only after Kafka acknowledgments. A crash between
acknowledgment and that state commit can duplicate messages; stable observation
IDs make the Bronze replay idempotent. The Spark consumer uses a dedicated topic,
lake and checkpoint. Source sample tables remain separate and intact.

Silver resolves the latest observed record for each source key; a key index tracks
previous creation months so corrections that move dates can update both affected
partitions. Gold request metrics refresh affected months, while daily ML features
operate on the small global daily series. The partial latest source day is retained
in raw/analytics coverage but excluded from model training.

The API is mutable and provides no reliable source update timestamp or transactional
snapshot. These are observations collected over time, not true CDC or guaranteed
historical state transitions. Matching counts are necessary completeness evidence,
not proof that all row contents came from the exact same source instant.

## Commands

```powershell
# The plan already exists for the authorized run; this does not download records.
.\.venv\Scripts\python.exe -m urbanlake.backfill status --root data/full/backfill
.\.venv\Scripts\python.exe -m urbanlake.full_project --status
# Resume the pipeline (downloads, Kafka, Spark and per-month finalization):
.\.venv\Scripts\python.exe -m urbanlake.full_run run --root data/full/backfill --lake data/full/lake --checkpoint data/full/checkpoints/kafka-v1 --topic urbanlake.full.observations.v1
# Complete source processing, global audit, model training and local promotion:
.\.venv\Scripts\python.exe -m urbanlake.full_project --prefetch
```

These commands resume the existing frozen historical plan. They do not expand
its scope to newer records after completion. Future manual updates need an
explicit new source window or plan; see the manual-run guidance in `README.md`.

## Live progress page

Open **http://localhost:8000/progress**. During a run it polls `/v1/progress` every two seconds,
shows actual downloaded/finalized record counts, per-month completion, current
work, rough ETA, and the later audit/training/serving phases. Browser failures
preserve the last actual snapshot rather than simulating movement. The overall
estimate weights download 30%, lakehouse 60%, audit 4%, training 4%, serving 2%;
only a verified complete project displays 100%.
This run now displays **100%**, with all 83 months finalized.

During development the existing API runs with `compose.progress.yaml` to read the
current source. The full-model promotion uses `compose.full.yaml`, preserving the
same page. HTTP checks verified successful page delivery, no-cache fresh snapshots,
the 83-month plan, monotonic record counters, and honest completion gating. UI
JavaScript syntax was checked; browser automation was unavailable, so visual
browser QA was not claimed.

`data/full/backfill/pipeline.json` and `pipeline-status.json` record pipeline stages,
completion and timing. Per-month source state is under `partitions/YYYY-MM/state.json`;
per-page Kafka state is `kafka.json`. Logs and all source/runtime artifacts are
Git-ignored. A full run is complete only after all planned source partitions,
Kafka relays, Delta coverage gates and independent full-lake validation succeed.

During heavy processing, only UrbanLake's own Airflow, MLflow, Prometheus and
Grafana were paused. The unrelated pre-existing Airflow project was left running.
UrbanLake's services were restored after processing. Final checks at **08:00:54
UTC** passed API health/readiness, MLflow health, Prometheus readiness, Grafana
database health and its **eight-panel dashboard**, and Airflow's metadatabase,
scheduler, and triggerer health. The unrelated Airflow project remained untouched.
At **08:01 UTC**, the actual container's DagBag had no import errors; all **three
DAGs** used `schedule=None`, `catchup=False`, and were explicitly paused. The
full-run Compose override mounts the current DAGs read-only so those settings are
the ones loaded by the restored service.
After this historical run, updates remain manual. Both reconciliation DAGs use
`schedule=None`; no daily laptop task or automatic update schedule was installed.
Compose Spark jobs use four local worker threads and a 2 GiB driver within the
existing 4 GiB container memory limit; the session defaults remain configurable.

## Verification status

Current local unit checks: **64 passed**; Ruff lint and format checks passed for
**43 files**. Spark replay and a correction moving a key
from January into February passed, including repairing both months' Silver/Gold.
January 2020 downloaded **195,231** records; before/after source counts agree and
the compressed archive is **61.19 MB**. Its actual Kafka/Delta pipeline passed:
all 195,231 source records were covered by the Bronze/Silver/Gold completion gate.
February downloaded completely (173,162 records). The full run uses one-month
prefetch and a combined Spark consumer/finalizer; all 83 source months have now
downloaded.

At 2026-11-04 03:10 UTC, Windows denied an atomic coordinator-status replacement
after 30 source months had downloaded and 29 months had finalized. The data was
intact. Shared state publication now uses unique same-directory temporary files,
flush/fsync, and bounded retries for transient sharing violations; permanent
failures still stop the run and preserve prior valid state. Four regression cases
cover retries, exhaustion, unrelated write errors, and temporary-file isolation.
The run resumed at 03:16 UTC from June 2022 without redownloading completed months.

At the first global audit, all lake layers reconciled with their archive at
**23,008,705** records, but the independent initial source reference was
**23,008,706**. The audit failed rather than accepting the one-row gap. A live
boundary probe at **07:48 UTC** reproduced the cause: SoQL interpreted the planned
exclusive end `2026-11-01T02:06:29.000001` at millisecond precision, excluding the
source record at `02:06:29.000` (key `12345678`). Querying the equivalent
millisecond-grid end `02:06:29.001` returned the full reference count.
Socrata's [floating timestamp documentation](https://dev.socrata.com/docs/datatypes/floating_timestamp)
specifies millisecond precision; the truncation behavior was established by the
saved live probe rather than assumed from the documentation.

The fix ceilings both generated query bounds to the source timestamp grid while
preserving the original logical plan and observed pagination cursors. Boundary
regression tests cover both bounds and inclusion of the final valid source row.
Before repair, manifests, the old failed audit, processing state, and Kafka relay
state were backed up with SHA-256 hashes under
`artifacts/boundary-recovery-20261104/`. Existing archives and the Kafka checkpoint
were preserved. Only the final month was reopened: its count changed from
**1,539 to 1,540**, and the missing row was appended as a checksummed archive page
and relayed through Kafka. Total archived source coverage is now **23,008,706**;
final-month Spark processing and the strict global audit both passed after repair.

## Final audit, model, and serving evidence

The independent audit passed with **23,008,706** archived observations, Bronze
rows, distinct observation IDs, distinct source keys, Silver rows/distinct keys,
Gold demand requests, and Gold analytics requests. It found **0 invalid requests**.
The lake contains **2,497 daily demand rows**; its partial latest source day,
November 1, was retained in analytics and excluded from training. The complete-day
export contains **2,496 days**, producing **2,482 feature rows**.

The full-history random forest was selected on validation and evaluated on
**497 chronological held-out days**. The development segment contains **1,985
rows**, including **397 validation rows**.

| Metric | Full-history result, requests/day |
|---|---:|
| Random-forest validation MAE | 772.7431 |
| Seasonal-naive validation MAE | 1,092.4307 |
| Random-forest test MAE | **788.3973** |
| Seasonal-naive test MAE | **1,089.7203** |
| Random-forest test RMSE | 1,166.5870 |

MLflow run **`LOCAL_RUN_ID`** reached **FINISHED**. The
promoted API reports model version **`LOCAL_MODEL_VERSION`** and the matching full-data
evaluation metadata. Actual Redis caching was verified: a repeated historical
November 1 forecast returned **10,922.37 requests** with `cached=true` and the
promoted model identity. This is a model output, not an observed count. Prometheus
reported full-run Bronze and Silver counts of **23,008,706**.

The source remains stale: the last complete training day is **October 31, 2026**,
and the supported forecast target is **November 1, 2026**, not the handoff date.
At verification the API explicitly reported `source_stale=true` and
`data_age_days=4`. Full-history completion does not create newer source records,
point-in-time history, confidence intervals, or a production-quality guarantee.
Updates remain manual. No GitHub push or cloud deployment was performed.

Primary current evidence is in `data/full/backfill/partitions/*/state.json`,
`data/full/backfill/pipeline.json`, `artifacts/full-project-status.json`,
`artifacts/full-boundary-probe.json`, and the boundary recovery manifest. The
authoritative successful audit is `artifacts/full-verification.json` with
`status=passed`. `artifacts/full-model.metrics.json` records evaluation, and
`artifacts/full-project-status.json` records the completed model, real cache check,
monitoring counts, and finish time. `artifacts/full-services-result.json` records
the successful final service checks and 100% progress snapshot. These generated
runtime artifacts remain Git-ignored.
