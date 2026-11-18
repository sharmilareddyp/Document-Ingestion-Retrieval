"""Re-check stored documents for chunk and index consistency."""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def _audit() -> dict:
    from app.orchestration.tasks import run_quality_audit

    return run_quality_audit()


with DAG(
    dag_id="data_quality",
    start_date=datetime(2024, 1, 1),
    schedule=os.environ.get("QUALITY_CRON", "30 * * * *"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=1),
    },
    tags=["quality"],
) as dag:
    PythonOperator(task_id="audit_corpus", python_callable=_audit)
