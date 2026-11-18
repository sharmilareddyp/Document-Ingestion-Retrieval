"""Force a full reprocess when parsers, chunking, or embeddings change."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def _backfill() -> dict:
    from app.orchestration.tasks import run_backfill

    return run_backfill()


with DAG(
    dag_id="full_backfill",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=6),
    },
    tags=["backfill"],
) as dag:
    PythonOperator(task_id="force_reprocess_sources", python_callable=_backfill)
