from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow/scripts")


DEFAULT_ARGS = {
    "owner": "chess-platform",
    "retries": 1,
    "retry_delay": timedelta(seconds=180),
}


def _materialize_current_month(ds: str) -> None:
    import bronze_service

    bronze_service.materialize_current_month(ds)


with DAG(
    dag_id="bronze_games_current_month_daily",
    description="Materialize canonical bronze game parquet for the active current month",
    start_date=datetime(2024, 1, 1),
    schedule_interval="45 1 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["chess", "bronze", "games", "current-month", "parquet"],
    max_active_runs=1,
    max_active_tasks=1,
) as dag:
    PythonOperator(
        task_id="materialize_current_month_bronze_games",
        python_callable=_materialize_current_month,
        op_kwargs={
            "ds": "{{ ds }}",
        },
    )
