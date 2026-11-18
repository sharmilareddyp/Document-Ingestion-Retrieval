"""Incremental ingestion across filesystem and API sources."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=2),
}


def _filesystem() -> dict:
    from app.orchestration.tasks import _ingest

    return _ingest(source_type="filesystem", trigger="schedule", force=False)


def _api() -> dict:
    from app.orchestration.tasks import _ingest

    return _ingest(source_type="rest_api", trigger="schedule", force=False)


with DAG(
    dag_id="incremental_processing",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args=default_args,
    tags=["ingestion"],
) as dag:
    filesystem = PythonOperator(task_id="incremental_filesystem", python_callable=_filesystem)
    api = PythonOperator(task_id="incremental_api", python_callable=_api)
    filesystem >> api
