"""Synchronize configured REST API sources."""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=1),
}


def _sync() -> dict:
    from app.orchestration.tasks import run_api_sync

    return run_api_sync()


with DAG(
    dag_id="api_sync",
    start_date=datetime(2024, 1, 1),
    schedule=os.environ.get("API_SYNC_CRON", "15 * * * *"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ingestion", "api"],
) as dag:
    PythonOperator(task_id="sync_api_sources", python_callable=_sync)
