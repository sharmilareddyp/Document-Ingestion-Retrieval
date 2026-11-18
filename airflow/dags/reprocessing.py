"""Retry documents that failed for a transient reason."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def _retry() -> dict:
    from app.orchestration.tasks import run_reprocessing

    return run_reprocessing()


with DAG(
    dag_id="failed_document_reprocessing",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(hours=2),
    },
    tags=["recovery"],
) as dag:
    PythonOperator(task_id="retry_failed_documents", python_callable=_retry)
