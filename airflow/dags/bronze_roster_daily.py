from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow/scripts")

TITLES = ("GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM")


def _materialize_title_roster_snapshot(title: str, ds: str) -> None:
    import bronze_service

    bronze_service.materialize_title_roster_snapshot(title=title, ds=ds)


DEFAULT_ARGS = {
    "owner": "chess-platform",
    "retries": 1,
    "retry_delay": timedelta(seconds=120),
}


with DAG(
    dag_id="bronze_roster_daily",
    description="Materialize structured bronze roster parquet from raw titled-player snapshots",
    start_date=datetime(2024, 1, 1),
    schedule_interval="20 0 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["chess", "bronze", "roster", "parquet"],
    max_active_runs=1,
    max_active_tasks=2,
) as dag:
    for title in TITLES:
        PythonOperator(
            task_id=f"bronze_roster_{title}",
            python_callable=_materialize_title_roster_snapshot,
            op_kwargs={
                "title": title,
                "ds": "{{ ds }}",
            },
        )
