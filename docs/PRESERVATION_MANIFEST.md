# UrbanLake preservation inventory

Audit date: **November 7, 2026**, America/New_York. This document records the
read-only archival audit and the subsequently authorized documentation work.
No backup, restoration, service shutdown, resource deletion, commit, or push was
performed as part of that work.

This is a public-safe inventory of preservation requirements, not a completed
backup manifest or a certificate that the project has been restored. Sizes are
logical file bytes measured locally; they do not measure allocated disk blocks,
Docker image sharing, or recoverable Windows disk space. Rows that describe
subsets overlap and must not be added together.

## Source and publication boundary

The public source repository is
[SaiSakethN/urbanlake](https://github.com/SaiSakethN/urbanlake), with the archival
audit referencing published commit
`db2b4464682eb5a285e72583eb656d2e6643400c`. Source publication does not preserve
the ignored dataset, trained models, experiment database, or deployment assets.
The new archival documents are not included in that already-published commit.
Committing and pushing them requires separate approval.

The original local `.git` history and the sanitized public history differ
intentionally. Retain the original repository. Do not add a public remote and
push its original branches blindly: older local history contains private
metadata that was removed from the published history. Do not rewrite either
history during cleanup.

| Resource | Classification | Logical bytes | Preservation requirement |
|---|---|---:|---|
| Tracked source, tests, SQL, DAGs, infrastructure, dependency definitions, and documentation | PRESERVE | Not separately measured here | Preserve the verified public commit and any reviewed subsequent changes. |
| Original `.git/` | PRESERVE; private local history | Not separately measured here | Retain until restoration checks pass and deletion is explicitly authorized. Any separate private backup needs approval. |
| `artifacts/github-public-final-20261104-152814/` | PRESERVE publication working copy | 851,098 | Contains the sanitized checkout and Git transport state; 117 local files at measurement. The GitHub commit is the authoritative public reference. |
| `artifacts/github-public-scan.json` | PRESERVE audit evidence | 331 | Records the prior publication privacy scan; it does not replace a scan of later changes. |
| `artifacts/github-publication-result.json` | PRESERVE privately; review before sharing | 1,400 | Retain publication traceability without copying private metadata into public documentation. |

## Captured source observations

Source: official [NYC 311 Service Requests](https://data.cityofnewyork.us/d/erm2-nwe9).
The frozen logical range is
`[2020-01-01T00:00:00, 2026-11-01T02:06:29.000001)`, in the source's NYC local
timestamp convention. It contains **23,008,706 observations across 83 monthly
partitions**. The final source day and month are partial.

The upstream API is mutable and has no established transactional snapshot or
reliable row-level update timestamp. Downloading the same date range again may
produce revised values or different totals. Consequently, these captured
observations are **not guaranteed to be reproducible from the current API**.
Preserve the archive if exact historical replay matters.

| Resource | Classification | Logical bytes | Dependencies and preservation requirement |
|---|---|---:|---|
| `data/full/backfill/` | PRESERVE; exact captured source and provenance | 7,317,322,552 | 5,066 files, including the archive pages, partition metadata, relay records, pipeline state, and logs. Preserve a coherent set rather than page files alone. |
| Gzip pages referenced by `data/full/backfill/partitions/*/state.json` | REQUIRES VERIFIED PRESERVATION before deletion | 7,313,221,792 | 4,642 compressed observation pages. Preserve their recorded paths and SHA-256 entries. Raw observations can include addresses, coordinates, and source request identifiers. |
| `data/full/backfill/plan.json` | PRESERVE frozen scope | 10,195 | Defines the 83 partitions and exclusive cutoff; do not replace it with a new live-source plan when reproducing this capture. |
| `data/full/backfill/partitions/*/state.json` | PRESERVE source manifests | Included above | All 83 manifests are complete and total 23,008,706 observations. Needed for page discovery, checksums, counts, cursors, and completion evidence. |
| `data/full/backfill/partitions/*/kafka.json` | PRESERVE historical relay evidence | Included above | Acknowledgements relate to the original broker. They must not be treated as proof that a new broker contains those messages. |
| `data/full/backfill/pipeline.json` | PRESERVE historical coordinator evidence | 61,550 | Records completed stages. Completed flags do not reconstruct deleted lake tables or a new broker. |
| `data/full/backfill/pipeline-status.json` | PRESERVE compact status evidence | 324 | Historical progress summary, not an independent dataset backup. |
| `data/full/backfill/status.json` | PRESERVE compact download evidence | 372 | Historical download status. |
| `artifacts/full-source-probe.json` | PRESERVE initial independent source reference | 830 | Required to retain the initial reference count used by the full verification script. A new source probe is a different observation. |

**Archive integrity was verified during this audit:** SHA-256 was streamed over
all **4,642 of 4,642 pages**, reading **7,313,221,792 bytes** in **66.47 seconds**.
There were **zero checksum mismatches, zero missing pages, and zero read
errors**. The check produced no report file. Page hashes remain in the existing
local manifests; this document does not duplicate their contents.

No captured license or terms snapshot was found in the reviewed project
documentation. The official dataset reference alone is not a redistribution
approval. Review applicable terms and privacy implications before transferring
or publishing raw observations. No raw source data belongs in the public Git
repository. A proposed private backup requires approval of its protection and
destination; no such backup was created by this audit.

## Models, analytical assets, and release

| Resource | Classification | Logical bytes | Dependencies and preservation requirement |
|---|---|---:|---|
| `artifacts/full-model.joblib` | PRESERVE actual trained model | 828,220 | Requires trusted provenance and the pinned Python/scikit-learn runtime. Joblib loading can execute code; do not load untrusted replacements. Exact regeneration is not guaranteed. |
| `artifacts/full-model.joblib.gz` | PRESERVE compact lossless model copy | 245,066 | Audit decompression matched the full model byte for byte. Retaining one verified copy is sufficient only after an approved preservation plan specifies which copy and how to restore it. |
| `artifacts/full-model.metrics.json` | PRESERVE historical evaluation | 447 | Records selected-model evaluation and the source cutoff. Evaluation values depend on the exact historical training series. |
| `data/full/lake/gold/demand_export/` | PRESERVE compact training input before lake removal | 41,006 | Four files, including the CSV export and supporting filesystem metadata. Contains 2,496 complete source days; excludes the partial November 1 source day. Preserve alongside model, metrics, and feature definitions. |
| `artifacts/public-analytics.json.gz` | PRESERVE hosted analytics input | 297,134 | Aggregate facts, not raw request records. Can be regenerated from the matching Gold tables, but preserve now to avoid rebuilding the full lake. |
| `artifacts/public-analytics.json.gz.sha256` | PRESERVE aggregate checksum | 65 | The checksum matched the gzip file during the audit. |
| `artifacts/application-release-20261104-145851/` | PRESERVE current deployable application bundle | 700,669 | 19 files containing application code/configuration, model, aggregate analytics, and verification metadata. Preserve its release manifest. It is not the full data platform or an independent off-device backup. |
| `artifacts/model.joblib` | PRESERVE until bounded-model retention decision | 268,489 | Earlier bounded-workflow model; distinct from the full-history model. |
| `artifacts/model.metrics.json` | PRESERVE bounded evaluation evidence | 439 | Companion to the earlier bounded model. |

The full model matched the completed serving evidence during the read-only
audit. The aggregate's monthly fact sum and daily sum each equaled
**23,008,706**, across **2,497 daily rows**. Every one of the current release's
**18 manifest-listed payload files** passed its recorded size/hash check, and
the release's compressed model matched the original full model. The nineteenth
file is the release manifest itself. These checks verify local integrity, not a
fresh deployment or full reconstruction.

The trained model's last complete source day is **October 31, 2026**, with a
supported historical forecast for **November 1, 2026**. Archival does not make
that model a current-day forecasting service.

## Experiments and verification evidence

| Resource | Classification | Logical bytes | Dependencies and preservation requirement |
|---|---|---:|---|
| UrbanLake PostgreSQL data volume: MLflow database | REQUIRES BACKUP or explicit retention | Docker-managed; not measured in this document | Holds experiment/run metadata and metrics. A consistent database backup or approved volume preservation is needed for complete experiment history. Do not publish database contents. |
| `artifacts/mlflow/` | PRESERVE with MLflow database | 1,350,263 | 22 artifact files. The artifact directory alone does not preserve the database's experiment/run records. |
| UrbanLake PostgreSQL data volume: Airflow database | REQUIRES USER REVIEW | Shares the UrbanLake PostgreSQL resource | Scheduler/task history differs from source code. A fresh migration can recreate schema, but not historical execution records. Review before deleting the containing volume. |
| `artifacts/full-verification.json` | PRESERVE successful full-lake audit | 891 | Historical `passed` evidence, including archive/Bronze/Silver/Gold count reconciliation. No full Spark audit was rerun during archival inspection. |
| `artifacts/full-project-status.json` | PRESERVE completion and promotion evidence | 1,693 | Records the completed run and verified model/serving result. Not a substitute for the actual model or data. |
| `artifacts/full-services-result.json` | PRESERVE historical service checks | 1,981 | Service checks succeeded during the completed build; they do not prove services are running now. |
| `artifacts/full-boundary-probe.json` | PRESERVE source precision incident evidence | 3,665 | Documents the exclusive-end timestamp precision investigation. |
| `artifacts/boundary-recovery-20261104/` | PRESERVE pre-existing incident recovery evidence | 75,712 | Seven pre-existing files. Retain the failed audit and repair provenance; this audit created no new backup. |
| `artifacts/public-application-verification.json` | PRESERVE historical deployed-app checks | 879 | Historical HTTP verification; not visual browser QA or a new live check. |
| Other deployment, upload, integration, and service reports under `artifacts/` | REQUIRES REVIEW; preserve selected evidence | Not separately measured here | Review before discarding. Some files contain private infrastructure identifiers or personal paths even when they contain no credential. Keep such records out of public documentation. |

No experiment database backup or database restoration was performed. A clean
MLflow/Airflow installation is not a restoration of their historical records.

## Derived lake and checkpoint state

These measurements are subsets of `data/full/lake/`. The full measured lake is
**16,530,676,046 bytes** across the listed stage subtrees, excluding the one-byte
writer lock. It can be derived from preserved observations and code,
but only a successful restoration check justifies relying on that statement for
deletion. The retained compact training export is included in Gold below and
must be extracted into the approved preservation set before any whole-Gold or
whole-lake removal.

| Resource | Classification | Logical bytes | Dependencies and preservation requirement |
|---|---|---:|---|
| `data/full/lake/bronze/` | CONDITIONALLY REGENERABLE; no deletion approved | 10,952,358,229 | Source observations plus Delta history. Rebuild only from verified preserved archives or complete Kafka replay. |
| `data/full/lake/silver/` | CONDITIONALLY REGENERABLE; no deletion approved | 3,768,512,889 | Depends on Bronze, reconciliation logic, and exact captured observations. Includes request state and observed changes. |
| `data/full/lake/gold/` | CONDITIONALLY REGENERABLE; preserve compact exports first | 28,033,746 | Depends on matching Silver and SQL definitions. Contains training inputs and analytics needed by later outputs. |
| `data/full/lake/_index/` | CONDITIONALLY REGENERABLE; no deletion approved | 1,781,748,608 | Source-key index used for affected-partition updates. Regeneration needs matching source/lake state. |
| `data/full/lake/_processing_state/` | PRESERVE evidence; not sufficient to restore tables | 22,574 | 83 per-month processing markers. Do not copy completed markers into an empty lake and assume reconstruction is complete. |
| `data/full/checkpoints/` | REQUIRES USER REVIEW; broker/lake-coupled state | 89,382 | 408 files. Spark Kafka offsets belong to their original stream, broker, and sink. Do not reuse blindly with a new broker or independent lake. |
| UrbanLake Kafka data volume | REQUIRES USER REVIEW | Docker-managed; not measured in this document | Retention-limited transport data and broker metadata. Preserved source pages support recovery, but old relay acknowledgements do not repopulate a new broker. |

The measured lake includes Delta transaction logs and may contain superseded
data files. Do not remove individual Parquet files or `_delta_log` entries to
reclaim space. No claim about disposable historical-file bytes was made.

The existing `urbanlake.bulk_processing load-files` mode can rebuild an isolated
lake directly from the archive after validating page hashes. That recovery path
does not use Kafka and must be identified as direct archive recovery. A separate
Kafka restoration needs compatible broker/checkpoint preservation or a reviewed
fresh relay/checkpoint procedure. Neither path was executed during this audit.

## Private configuration and disposable candidates

| Resource | Classification | Preservation requirement |
|---|---|---|
| `deploy/vercel/project.json` | PRIVATE; REQUIRES USER REVIEW | Contains local deployment linkage. Prefer relinking an authorized account where feasible; otherwise use approved secure preservation. Keep the public example template empty of private identifiers. |
| `artifacts/airflow/standalone_admin_password.txt` | PRIVATE; usually regenerable | Do not publish or display contents. Decide whether the old local login remains necessary; a fresh installation can create new credentials. |
| `artifacts/airflow/airflow.cfg` and generated webserver configuration | PRIVATE-CONFIG REVIEW | Inspect only necessary settings. Preserve required references or a sanitized template; do not copy secret-bearing configuration into public docs. |
| `artifacts/github-public-redactions.json` | PRIVATE; PRESERVE until publication workflow is reviewed | Local mapping used for sanitized publication. Never publish its contents or use it to rewrite history without approval. |
| Other ignored `.env*`, key, token, connection, or account metadata if present | PRIVATE; REVIEW individually | Do not assume every credential is needed or every ignored file is safe. Shared account credentials must remain with their existing manager. |
| `.env.example`, `deploy/vercel/project.example.json` | PRESERVE safe templates | Retain variable names and placeholders; do not insert live private values. |
| `.venv/`, Python bytecode, test/lint caches, fixture lakes, temporary releases, and temporary logs | CLEANUP CANDIDATES after approval and review | Dependencies or fixtures are generally reproducible. Review all untracked outputs first; do not delete selected evidence, the retained current release, or unique artifacts by a broad `artifacts/` or `data/` deletion. |
| Other projects, shared tools, shared Docker resources, and WSL distributions | SHARED RESOURCE — DO NOT TOUCH | No ownership or deletion authorization follows from this project inventory. |

Public source does not require private account identifiers or old generated
passwords. No credential values, private model/run identifiers, personal machine
paths, or private preservation destinations are included in this document.

## What remains before deletion can be approved

1. Agree on which exact snapshot, models, experiment records, and evidence must
   survive cleanup.
2. Approve any private backup method and destination separately. Verify backups
   after creation; do not remove them in the operation that creates them.
3. Follow [the archival and restore guide](ARCHIVAL_AND_RESTORE.md), using an
   isolated restoration location and explicit resource limits.
4. Verify the selected restoration mode and match source counts, checksums,
   analytical totals, model input/evaluation expectations, and application
   behavior as applicable. Document any transport path that was not exercised.
5. Review the exact Docker ownership/dependency findings and exact local paths
   in the cleanup proposal. Approval for documentation is not approval to stop
   services, remove resources, compact WSL, or publish changes.
6. Obtain explicit cleanup approval, then measure actual free space before and
   after the approved batches. Logical file sizes above are not a recovery
   measurement.

Until those checks and approvals are complete, this inventory authorizes no
deletion and makes no claim that a separate, restorable archive exists.
