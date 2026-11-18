"""Probe REST sources for schema drift without indexing the sample page."""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def _probe() -> dict:
    from app.orchestration.tasks import run_schema_probe

    return run_schema_probe()


with DAG(
    dag_id="schema_drift",
    start_date=datetime(2024, 1, 1),
    schedule=os.environ.get("DRIFT_CRON", "45 * * * *"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["schema"],
) as dag:
    PythonOperator(task_id="probe_schema_contracts", python_callable=_probe)
