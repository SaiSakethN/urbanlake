# Full-data extension

The initial 120-minute build was completed and audited separately. On November 3,
2026 the user then explicitly authorized processing the complete dataset. This
document records that new full-data phase; the earlier sample audit is preserved.

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
multi-hour full-data run is expected; no premature completion claim is made.

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
preserving source timestamp precision. It commits compressed pages atomically,
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
# Resume the pipeline (downloads, Kafka, Spark and per-month finalization):
.\.venv\Scripts\python.exe -m urbanlake.full_run run --root data/full/backfill --lake data/full/lake --checkpoint data/full/checkpoints/kafka-v1 --topic urbanlake.full.observations.v1
# Complete source processing, global audit, model training and local promotion:
.\.venv\Scripts\python.exe -m urbanlake.full_project --prefetch
```

`data/full/backfill/pipeline.json` and `pipeline-status.json` record pipeline stages,
completion and timing. Per-month source state is under `partitions/YYYY-MM/state.json`;
per-page Kafka state is `kafka.json`. Logs and all source/runtime artifacts are
Git-ignored. A full run is complete only after all planned source partitions,
Kafka relays, Delta coverage gates and independent full-lake validation succeed.

During heavy processing, only UrbanLake's own Airflow, MLflow, Prometheus and
Grafana were paused. The unrelated pre-existing Airflow project was left running.
These services will be restored for final model tracking and serving validation.
Compose Spark jobs use four local worker threads and a 2 GiB driver within the
existing 4 GiB container memory limit; the session defaults remain configurable.

## Verification status

Full-data unit checks: **44 passed**. Spark replay and a correction moving a key
from January into February passed, including repairing both months' Silver/Gold.
January 2020 downloaded **195,231** records; before/after source counts agree and
the compressed archive is **61.19 MB**. Its actual Kafka/Delta pipeline passed:
all 195,231 source records were covered by the Bronze/Silver/Gold completion gate.
February also downloaded completely (173,162 records); processing is continuing
through all 83 months with one-month prefetch and a combined Spark consumer/finalizer.

Full 23-million-row completion remains pending and must be backed by actual final
counts and evidence. `full_project` refuses to promote a model before the global
source/lake audit passes; it then trains on all complete historical days, restores
MLflow/monitoring, loads the full-history model, and verifies its identity, actual
Redis cache hit and FINISHED MLflow run. The old sample model remains available
until this gated promotion succeeds. No GitHub push or cloud deployment is included.
