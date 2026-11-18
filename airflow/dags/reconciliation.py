"""Replay vector outbox rows that did not reach Pinecone."""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def _reconcile() -> dict:
    from app.orchestration.tasks import run_reconciliation

    return run_reconciliation()


with DAG(
    dag_id="pinecone_reconciliation",
    start_date=datetime(2024, 1, 1),
    schedule=os.environ.get("RECONCILIATION_CRON", "*/15 * * * *"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["recovery"],
) as dag:
    PythonOperator(task_id="flush_indexing_outbox", python_callable=_reconcile)
