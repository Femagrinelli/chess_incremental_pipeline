from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow/scripts")

TITLES = ("GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM")


def _bootstrap_title_state_from_legacy_raw(title: str, ds: str) -> None:
    import ingestion_service

    ingestion_service.bootstrap_title_state_from_legacy_raw(title=title, ds=ds)


DEFAULT_ARGS = {
    "owner": "chess-platform",
    "retries": 0,
}


with DAG(
    dag_id="bootstrap_legacy_raw_state",
    description="One-time bootstrap of state files from the existing legacy raw S3 layout",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["chess", "bootstrap", "state"],
    max_active_runs=1,
    max_active_tasks=2,
) as dag:
    for title in TITLES:
        PythonOperator(
            task_id=f"bootstrap_{title}",
            python_callable=_bootstrap_title_state_from_legacy_raw,
            op_kwargs={
                "title": title,
                "ds": "{{ ds }}",
            },
        )
