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
    import gold_service

    gold_service.materialize_current_month(ds)


with DAG(
    dag_id="gold_title_month_current_daily",
    description="Materialize current-month gold title-month marts from silver parquet",
    start_date=datetime(2024, 1, 1),
    schedule_interval="30 5 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["chess", "gold", "metrics", "current-month"],
    max_active_runs=1,
    max_active_tasks=1,
) as dag:
    PythonOperator(
        task_id="materialize_current_month_gold",
        python_callable=_materialize_current_month,
        op_kwargs={
            "ds": "{{ ds }}",
        },
    )
