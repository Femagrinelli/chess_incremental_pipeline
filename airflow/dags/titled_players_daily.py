from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow/scripts")

import chess_client
import ingestion_service


DEFAULT_ARGS = {
    "owner": "chess-platform",
    "retries": 3,
    "retry_delay": timedelta(seconds=60),
}


with DAG(
    dag_id="titled_players_daily",
    description="Daily CDC snapshot for each titled player roster",
    start_date=datetime(2024, 1, 1),
    schedule_interval="5 0 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["chess", "bronze", "roster"],
    max_active_runs=1,
    max_active_tasks=2,
) as dag:
    for title in chess_client.VALID_TITLES:
        PythonOperator(
            task_id=f"snapshot_{title}",
            python_callable=ingestion_service.run_titled_players_snapshot,
            op_kwargs={
                "title": title,
                "ds": "{{ ds }}",
            },
        )
