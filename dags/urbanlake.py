"""Paused-by-default bounded workflows; no implicit full-dataset backfills.

The application CLI runs in its own Python environment, isolated from Airflow.
All medallion writers use one shared Airflow pool slot / max_active_runs=1;
do not run manual Spark writers at the same time.
"""

import hashlib
import subprocess
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task


def execute(*arguments):
    subprocess.run(["/usr/local/bin/urbanlake", *arguments], cwd="/app", check=True)


@dag(
    dag_id="urbanlake_recent_reconciliation",
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["urbanlake", "bounded", "not-cdc"],
)
def recent():
    @task
    def ingest_requests(data_interval_end=None, run_id=None):
        # Poll three created-date days, excluding the source's most recent partial days.
        end = data_interval_end.in_timezone("America/New_York").date() - timedelta(days=3)
        token = hashlib.sha256(run_id.encode()).hexdigest()[:20]
        for days in range(3, 0, -1):
            start = end - timedelta(days=days)
            execute(
                "ingest",
                "--start",
                str(start),
                "--end",
                str(start + timedelta(days=1)),
                "--max-records",
                "20000",
                "--run-id",
                f"recent-{token}-{start}",
            )

    @task
    def ingest_demand(data_interval_end=None, run_id=None):
        end = data_interval_end.in_timezone("America/New_York").date() - timedelta(days=3)
        token = hashlib.sha256(run_id.encode()).hexdigest()[:20]
        execute(
            "ingest",
            "--kind",
            "daily_demand",
            "--start",
            str(end - timedelta(days=120)),
            "--end",
            str(end),
            "--run-id",
            f"demand-{token}",
        )

    @task
    def medallion():
        execute("process")

    @task
    def fit_model():
        execute("train")

    ingest_requests() >> ingest_demand() >> medallion() >> fit_model()


@dag(
    dag_id="urbanlake_wider_reconciliation",
    schedule="0 8 * * 0",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["urbanlake", "wider-reconciliation"],
)
def wider():
    @task
    def reconcile(data_interval_end=None, run_id=None):
        # Revisit a wider 14-day created-date window weekly. This DAG starts paused:
        # enabling it authorizes at most 280,000 records per run (20k/day cap).
        end = data_interval_end.in_timezone("America/New_York").date() - timedelta(days=3)
        token = hashlib.sha256(run_id.encode()).hexdigest()[:20]
        for days in range(1, 15):
            start = end - timedelta(days=days)
            execute(
                "ingest",
                "--start",
                str(start),
                "--end",
                str(start + timedelta(days=1)),
                "--max-records",
                "20000",
                "--run-id",
                f"wide-{token}-{start}",
            )
        execute("process")

    reconcile()


recent()
wider()
