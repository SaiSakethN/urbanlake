# UrbanLake archival and restoration

This runbook records the read-only audit performed on November 7, 2026
(America/New_York). The subsequent approval covers this documentation and the
[preservation inventory](PRESERVATION_MANIFEST.md) only. No backup, restoration,
shutdown, cleanup, commit, push, or deployment was performed for this change.

Commands below are instructions for a later approved operation. They have been
checked against the source and configuration; they have **not** been executed as
a fresh restoration drill. Do not run the whole document as a script.

## 1. Verified source and preservation boundary

Public repository: <https://github.com/SaiSakethN/urbanlake>.

The verified application-source baseline is
`db2b4464682eb5a285e72583eb656d2e6643400c`: 25 sanitized commits and 90 tracked
files. All 90 intended local source files match that public tree after the
existing privacy substitutions. Both jobs in [the published CI run](https://github.com/SaiSakethN/urbanlake/actions/runs/41858139499)
passed. These archival documents are a later local documentation change and are
not part of that baseline or published until a separate commit/push is approved.

The original development repository intentionally has no remote. Its historical
commits contain personal metadata and private resource identifiers. Preserve it
privately; do not attach a remote and push its original history. Retain the
sanitized publication checkout and private redaction mapping for future approved
publication. Do not rewrite either history as part of cleanup.

GitHub contains source, tests, infrastructure and documentation. It does not
contain the ignored source archive, lake, trained models, experiment database,
runtime credentials, or deployable release assets. A live hosted deployment is
not an archival backup.

## 2. What must be preserved first

Use the [inventory](PRESERVATION_MANIFEST.md) to review each resource. Essential
groups are:

- The exact source archive, original plan, partition manifests and source probe.
- The full trained model and metrics, training export, aggregate analytics,
  checksums, current application release and verification evidence.
- PostgreSQL experiment metadata **and** `artifacts/mlflow`; either alone is an
  incomplete MLflow archive.
- Airflow database/configuration when retaining its history or encrypted
  connections. Its generated login can instead be regenerated where appropriate.
- Original private Git history and the sanitized publication materials.

The archive contains 23,008,706 observations in 83 monthly partitions. All 4,642
compressed pages, totaling 7,313,221,792 bytes, passed a complete SHA-256 comparison
with their recorded manifest hashes during the audit. There were zero missing
pages, mismatches or read errors. This checks current local integrity; it does
not prove that a separate backup exists or can be restored.

The official source is [NYC 311](https://data.cityofnewyork.us/d/erm2-nwe9).
Retain source/version attribution and the applicable [NYC Open Data terms](https://data.cityofnewyork.us/stories/s/Terms-of-Use/k9k7-3cje).
Those terms explain that data can change and older versions are not retained.
Do not assign an invented license or promise an identical future download.
Raw requests can include addresses and coordinates; keep them out of public Git.

Private backups require approval of an encrypted destination and a retention
policy. A folder being outside Git does not make it secure. No destination or
credential value belongs in this public document. Preserve necessary secrets
through an approved credential manager or encrypted archive; regenerate local
credentials that are not worth retaining. Never display their values.

For a later approved backup, quiesce the relevant writers first. Export logical
PostgreSQL backups with PostgreSQL 15 tools, including both the `urbanlake` and
`airflow` databases if their histories are retained. Capture matching MLflow
artifacts and any required Airflow encryption configuration. Do not tar a live
PostgreSQL data directory and call it a consistent backup. Verify the backup's
file checksums and restore it into a separate destination before deleting an
original. Never delete a newly created backup in the same operation.

## 3. Tools and an isolated source checkout

Recorded build versions are Python 3.12.10, Java 17 in Docker, Spark 4.0.1,
Delta Lake 4.0.0, Kafka 4.0.1, Airflow 2.11.0, PostgreSQL 15.18,
Scikit-learn 1.7.1 and MLflow 3.2.0. The audit used Git 2.54.0 and Docker Compose
5.1.3 on Windows 11. Use Python 3.12, not an unverified default interpreter.
Java and Spark need not be installed on Windows.

`pyproject.toml` pins the primary dependencies. Dockerfiles and image references
record the container dependencies, including the Kafka distribution checksum.
There is no complete transitive hashed lockfile or offline dependency archive.
A future build still depends on upstream package/image availability. Capture a
reviewed environment inventory and any needed image exports in a separately
approved preservation operation.

After approving installation and restoration into a new location:

```powershell
if (Test-Path -LiteralPath urbanlake-restore) { throw 'Choose a new restore directory.' }
git clone https://github.com/SaiSakethN/urbanlake.git urbanlake-restore
if ($LASTEXITCODE -ne 0) { throw 'Clone failed.' }
Set-Location urbanlake-restore
git checkout --detach db2b4464682eb5a285e72583eb656d2e6643400c
if ($LASTEXITCODE -ne 0) { throw 'Baseline checkout failed.' }
py -3.12 -m venv .venv
if ($LASTEXITCODE -ne 0) { throw 'Python environment creation failed.' }
.\.venv\Scripts\python.exe -m pip install -e '.[dev]'
if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed.' }
.\.venv\Scripts\python.exe -m pip check
docker compose -p urbanlake-restore config --quiet
```

Use a new restore directory, distinct Compose project and distinct output paths.
An isolated project name alone does not prevent port collisions: the Compose
files still publish fixed host ports. Inspect listeners before starting a
service, or use an approved restore-only port override. Do not stop another
project to free its port. The one-shot Spark commands below use `--no-deps` and
do not publish service ports.

Image tags such as `urbanlake-spark:local` are also global across Compose project
names. The build examples below use those existing tags. Before any build,
review their consumers and approve any retagging, or use a separately approved
restore-only Compose override with distinct image tags and include it in the
commands. A different `-p` value does not isolate image tags.

If an environment file is needed, copy `.env.example` to a new `.env` without
overwriting one that already exists, then obtain required values privately.
The Socrata token is optional. Development defaults are not production secrets.
Do not print resolved Compose configuration, connection strings or environment
variables. `config --quiet` validates without displaying those values.

## 4. Restore the packaged web application

This is the smallest restoration: it needs the trusted current application
release, not Kafka, Spark, PostgreSQL, Redis, or the full lake. The preserved
release is `artifacts/application-release-20261104-145851` and contains application
source, configuration, compressed model, analytics, project/progress snapshots,
and `assets/release.json`. It is private archival material; do not add the
existing bundle or its internal identifiers to public Git.

Restore that directory from the approved encrypted archive into the isolated
checkout. Verify each of the 18 payload entries in `assets/release.json` against
its listed size and SHA-256 before executing the bundle. Verify path containment
and reject missing, unexpected or untrusted files. The audit checked all 18
entries successfully. `build.py` separately verifies the decompressed model;
it is not a substitute for checking the other payloads. Only load trusted joblib
models: deserialization can execute code.

In a new PowerShell session, from the isolated checkout:

```powershell
$restorePython = (Resolve-Path -LiteralPath .venv\Scripts\python.exe).Path
$appRelease = (Resolve-Path -LiteralPath artifacts\application-release-20261104-145851).Path
Set-Location -LiteralPath $appRelease
& $restorePython build.py
if ($LASTEXITCODE -ne 0) { throw 'Model verification/decompression failed.' }
$env:MODEL_PATH = Join-Path $appRelease 'assets/full-model.joblib'
$env:PUBLIC_ANALYTICS_PATH = Join-Path $appRelease 'assets/analytics.json.gz'
$env:PROJECT_OVERVIEW_PATH = Join-Path $appRelease 'assets/project.json'
$env:PROGRESS_SNAPSHOT_PATH = Join-Path $appRelease 'assets/progress.json'
$env:REDIS_ENABLED = 'false'
& $restorePython -m uvicorn app:app --host 127.0.0.1 --port 18000
```

Choose another unused local port if necessary. Close this session when finished
so these environment settings do not carry into other project work.

From another terminal, verify `/`, `/ready`, `/v1/project`,
`/v1/analytics/metadata`, filtering and matching CSV exports, and the supported
historical forecast. The API documentation is at `/docs`. `/ready` should report
`ready` and cache `disabled`; project metadata should report a passed audit.
Check actual answers and source counts, not only an HTTP 200 on the HTML shell.
The app has five views: Overview, Explore, Forecast, Data quality and Architecture.
Use the model's reported supported target date; do not invent a current forecast.

This app serves the recorded snapshot. It does not prove local pipeline recovery
or turn the November 2026 source cutoff into current data. Publishing it again
requires separate approval and rechecking the hosting budget and account scope.

## 5. Rebuild the exact observed lake from the source archive

Restore the complete `data/full/backfill` tree, `artifacts/full-source-probe.json`
and the required original evidence into the isolated checkout, preserving
relative page paths and metadata bytes. Do not replace the original working
tree. Retain any original acknowledgements as historical evidence, not proof of
a new Kafka transport.

Check that a new output lake does not already contain data, allow space for a
second approximately 16.53 GB derived lake plus temporary Spark work, and run
only one writer. Keep at least the agreed free-space reserve. Do not build/run
Spark concurrently with the entire service stack on a 16 GB machine.

After separate approval of the compute/storage cost, build the existing Spark
image and load the complete partitions in plan order:

```powershell
if (Test-Path -LiteralPath data\restored\lake) { throw 'Use a new restoration lake.' }
docker compose -p urbanlake-restore build spark
if ($LASTEXITCODE -ne 0) { throw 'Spark image build failed.' }
$restorePlan = Get-Content -LiteralPath data/full/backfill/plan.json -Raw | ConvertFrom-Json
foreach ($partition in $restorePlan.partitions) {
    docker compose -p urbanlake-restore run --rm --no-deps --entrypoint python `
      -e PYTHONPATH=/app/src `
      -v ./src:/app/src:ro -v ./sql:/app/sql:ro `
      -v ./data/full/backfill:/app/data/full/backfill:ro `
      spark -m urbanlake.bulk_processing load-files `
      --input-root /app/data/full/backfill `
      --lake /app/data/restored/lake --partition $partition.partition_id
    if ($LASTEXITCODE -ne 0) { throw "Restore failed for $($partition.partition_id)." }
}
```

`load-files` validates the completed source manifest and page hashes, writes
Bronze, and finalizes Silver, Gold, the source-key index and processing markers.
It does not use Kafka offsets. Keep the archive mount read-only. These commands
are source-reviewed instructions, not an executed full restoration result.

Run the independent count/coverage audit into a **new** evidence file:

```powershell
if (Test-Path -LiteralPath artifacts\restore-verification.json) { throw 'Choose a new audit report.' }
docker compose -p urbanlake-restore run --rm --no-deps --entrypoint python `
  -e PYTHONPATH=/app/src `
  -v ./src:/app/src:ro -v ./sql:/app/sql:ro -v ./scripts:/app/scripts:ro `
  -v ./data/full/backfill:/app/data/full/backfill:ro `
  spark /app/scripts/verify_full.py `
  --root /app/data/full/backfill --lake /app/data/restored/lake `
  --reference /app/artifacts/full-source-probe.json `
  --report /app/artifacts/restore-verification.json
if ($LASTEXITCODE -ne 0) { throw 'Restored-lake audit failed.' }
```

Required historical results include 23,008,706 Bronze observations and distinct
source keys, matching unique Silver requests and Gold request totals, 2,497
daily demand rows, and 2,496 complete-day training-export rows. The source's last
partial day is excluded from training. Check the full report and its status.

The audit also requires the retained `kafka.json` completion metadata. That
checks historical relay evidence; passing it after `load-files` does **not**
verify fresh Kafka delivery. Do not manufacture acknowledgements to pass it.

Never remove individual Parquet files or Delta logs to reclaim space. Preserve
complete tables and their logs together, or later remove an explicitly approved
derived lake as a whole after its replacement has passed verification.

## 6. Kafka, live-source regeneration and retraining

Choose the desired outcome before using the full-run orchestrator:

- **Same captured observations:** use the archive recovery path above.
- **Same broker state:** restore a reviewed, consistent broker-volume and
  matching checkpoint/relay backup. This requires a separately tested procedure.
- **New broker replay:** use fresh broker/checkpoint state and a separately
  reviewed replay plan. Old relay acknowledgements can skip publication; old
  offsets can point to missing data. This path has not been restore-drilled.
- **New upstream dataset:** use the README's fresh-clone probe, plan, full-history
  topic creation and ingestion instructions in a separate scope. The full topic
  is `urbanlake.full.observations.v1`, with two partitions and two-day retention.
  `kafka-init` alone creates the separate bounded-workflow topic.

Do not invoke `urbanlake.full_project --prefetch` as a read-only verification or
as an automatic restoration after deleting Kafka. It writes pipeline state,
trains/promotes a model, and starts/verifies services. Full online ingestion can
take hours and may produce different totals and metrics because the API changes.

The existing bounded fixture and replay commands are in the [README](../README.md).
They can verify Kafka/Spark transport on isolated data after approval; they do
not establish that all historical messages have been replayed to a new broker.

For optional retraining from the restored complete-day export, use a new model
output and an approved MLflow destination; do not overwrite the preserved model:

```powershell
# Run only after an isolated MLflow service is configured and healthy.
if (Test-Path -LiteralPath artifacts\restore-model.joblib) { throw 'Choose a new model output.' }
docker compose -p urbanlake-restore run --rm --no-deps train train `
  --input /app/data/restored/lake/gold/demand_export `
  --output /app/artifacts/restore-model.joblib
```

The inherited tracking URI must resolve to that project's MLflow service.
Review output existence first. Retain the historical full-model metrics and
baseline comparison separately; retraining is not guaranteed to reproduce the
same serialized model bytes.

## 7. PostgreSQL, MLflow and Airflow restoration

Use a fresh restore-project PostgreSQL volume and the same PostgreSQL major
version. `infra/postgres/init.sql` creates the separate Airflow database during
first initialization; it does not rerun against an existing initialized volume.
Start only the approved restore service and wait for readiness:

```powershell
docker compose -p urbanlake-restore up -d --wait postgres
```

Restore the approved logical database dumps into empty destinations with
PostgreSQL 15 `pg_restore`, using the configured owner and without echoing
credentials. A consistent dump of each database and verified encrypted backup
must exist first. Copy binary dumps using file-aware tools such as `docker cp`;
do not stream a binary dump through Windows PowerShell 5 text redirection.
Do not use `--clean` against an existing database or overwrite a volume.

Restore matching `artifacts/mlflow` files at their original container-relative
paths, then start the isolated MLflow service and verify experiment/run metadata,
metrics, and artifact download. Restoring files alone does not reconstruct its
PostgreSQL records. Start only one MLflow service against a given restored state.

For Airflow, review migrations with the pinned Airflow 2.11.0 environment before
starting the scheduler. Preserve any needed Fernet key together with encrypted
connection records. Regenerate an administrative login where possible; never
publish its generated password or a private `airflow.cfg`. Verify DAG import,
manual scheduling and paused states before enabling execution. All updates
remain manual; do not install a schedule as part of restoration.

## 8. Verification and publication gates

In the isolated checkout, after approval of the generated test outputs:

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m ruff format --check .
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m pip check
docker compose -p urbanlake-restore config --quiet
```

Tests create temporary state. Docker integration commands in the README also
create topics, lakes and reports. Run them only with reviewed isolation and keep
their new results distinct from historical evidence. Earlier documentation's
64-test or pre-publication statements describe earlier build phases; the final
recorded local suite had 111 passing tests and the published CI jobs passed.
Neither is a claim that tests were rerun for this documentation change.

Before any later commit or push, review the diff and scan staged/current files
and reachable publication history. Preserve `.gitignore` exclusions for data,
artifacts, environments and private deployment linkage. Do not commit this
inventory's referenced runtime files. Use the separately reviewed sanitized
publication workflow; do not directly push the original private history.
Obtain explicit commit/push approval and verify the actual remote result.

## 9. Safe shutdown, cleanup and measured recovery

No shutdown or deletion is authorized by this documentation approval. A later
cleanup approval must identify exact targets, retention decisions and commands.
Recheck project labels, mounts, consumers, backups and the remote commit before
each batch; reject symlinks/reparse targets outside the approved project scope.

For a later approved stop of the original project's nine audited services, the
explicit target list is:

```powershell
# Reference only: requires separate stop approval and fresh ownership checks.
docker stop urbanlake-api-1 urbanlake-pipeline-exporter-1 `
  urbanlake-airflow-1 urbanlake-mlflow-1 urbanlake-postgres-1 `
  urbanlake-kafka-1 urbanlake-redis-1 urbanlake-prometheus-1 urbanlake-grafana-1
```

Stopping preserves volumes but interrupts local endpoints. The hosted app uses
packaged assets independently. Do not stop Docker Desktop, WSL, unrelated Python
or Java processes, or other projects. A restore terminal's foreground API can be
stopped with Ctrl+C after confirming that it is the intended process.

Potential later file cleanup is limited to the verified `.venv`, cache directories
and the derived lake after its preservation/restore gates pass. Unreadable test
folders and unlabeled volumes remain under review. No blanket recursive deletion
or volume-removal command is provided here. Check resolved absolute targets and
show exact native PowerShell commands before executing an approved deletion.

Keep the shared PostgreSQL image, other projects' resources and global Docker
build cache. Never use global Docker prune commands. Container log sizes were not
measured, so no log-recovery estimate is included.

Audit measurements: at least 24.63 GB of project files; 142.00 GB free on C: at
the recorded sample; a 16.53 GB derived lake and 0.764 GB environment; nine
UrbanLake containers using about 2.21 GiB of working memory. About 17.29 GB of
project files is a conditional cleanup estimate, not recovered space. Some test
folders were inaccessible, so the project total is a lower bound.

Docker's shared data VHDX occupied 44.36 GB. Removing images/volumes can reclaim
space inside it without shrinking the Windows file. WSL shutdown or virtual-disk
compaction affects shared infrastructure and requires its own safety review and
approval. Do not unregister/reset a distribution or compact a live disk.

After approved cleanup, record actual Windows free space, Docker storage and
process/container memory before and after, with timestamps. Record remaining
resources and other projects' health. Do not count a Docker-internal reclaim
estimate as Windows disk space recovered. The original repository remains until
all restoration checks pass and its removal is explicitly authorized.

## 10. Outstanding work

At the November 7 documentation checkpoint, encrypted backup creation, consistent
database export, backup restoration, an isolated full rebuild, fresh-broker replay
and final archival publication remained unapproved or unexecuted.

On November 21, the approved disposable-file batch removed only `.venv`,
`.pytest_cache`, `.pytest_tmp` and `.ruff_cache`, after checking path containment,
reparse points, package ownership and preservation of dependency definitions. An
installed-version inventory was retained privately. The deleted logical files
totaled 763,570,780 bytes; Windows free space increased by 811,425,792 bytes during
the measured batch. Other activity can also affect free-space measurements.
Dataset, artifact, database, Git and Docker resources were not deleted or stopped.
The restricted test-output folders were retained.

Password-encrypted backup creation was subsequently approved, but no archive had
been created or verified at this follow-up checkpoint. The prepared local
interactive helper requires the owner to enter a password directly into 7-Zip;
neither passwords nor private backup destinations belong in this public guide.
Docker's API returned server errors, so container ownership refresh, shutdown,
database export and a restored-lake drill could not be completed. No shared Docker
or WSL restart was attempted. The large derived lake and database volumes remain
intact pending preservation and restoration checks.

These documents do not certify that UrbanLake has been fully archived or cleaned
up. Remaining actions need their applicable approvals and executed verification;
the limited disposable-file cleanup is the only disk recovery recorded here.
