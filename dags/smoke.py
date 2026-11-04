"""Manual smoke DAG: reuses the saved bounded data and performs no online fetch."""

import subprocess
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="urbanlake_saved_sample_smoke",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=10)},
    tags=["urbanlake", "manual-smoke", "no-online-download"],
)
def smoke():
    @task
    def publish():
        subprocess.run(
            [
                "/usr/local/bin/python",
                "/app/scripts/replay.py",
                "/app/data/samples/demand.jsonl",
                "/app/data/samples/requests.jsonl",
            ],
            cwd="/app",
            check=True,
        )

    @task
    def medallion():
        subprocess.run(["/usr/local/bin/urbanlake", "process"], cwd="/app", check=True)

    @task
    def forecast_training():
        subprocess.run(["/usr/local/bin/urbanlake", "train"], cwd="/app", check=True)

    publish() >> medallion() >> forecast_training()


smoke()
