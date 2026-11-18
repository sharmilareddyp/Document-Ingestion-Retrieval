"""Scheduled filesystem and upload-directory ingestion."""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=2),
}


def _ingest() -> dict:
    from app.orchestration.tasks import run_scheduled_ingestion

    return run_scheduled_ingestion()


def _quality() -> dict:
    from app.orchestration.tasks import run_quality_audit

    return run_quality_audit()


with DAG(
    dag_id="document_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule=os.environ.get("INGESTION_CRON", "0 * * * *"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ingestion"],
) as dag:
    ingest = PythonOperator(task_id="ingest_filesystem_sources", python_callable=_ingest)
    quality = PythonOperator(task_id="audit_data_quality", python_callable=_quality)
    ingest >> quality
