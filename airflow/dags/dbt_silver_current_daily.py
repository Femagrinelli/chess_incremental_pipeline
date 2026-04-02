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


def _build_current_silver(ds: str) -> None:
    import dbt_runner

    dbt_runner.build_silver_current(ds)


with DAG(
    dag_id="dbt_silver_current_daily",
    description="Build current bronze-to-silver models in dbt for the active roster snapshot and month",
    start_date=datetime(2024, 1, 1),
    schedule_interval="15 5 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["chess", "dbt", "silver", "current"],
    max_active_runs=1,
    max_active_tasks=1,
) as dag:
    PythonOperator(
        task_id="build_current_silver_models",
        python_callable=_build_current_silver,
        op_kwargs={
            "ds": "{{ ds }}",
        },
    )
