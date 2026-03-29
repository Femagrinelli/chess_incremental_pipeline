from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow/scripts")

import chess_client
import ingestion_service


DEFAULT_ARGS = {
    "owner": "chess-platform",
    "retries": 1,
    "retry_delay": timedelta(seconds=300),
}


with DAG(
    dag_id="player_games_backfill",
    description="Drain the historical month queue gradually",
    start_date=datetime(2024, 1, 1),
    schedule_interval="30 2 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["chess", "bronze", "games", "backfill"],
    max_active_runs=1,
    max_active_tasks=2,
) as dag:
    for title in chess_client.VALID_TITLES:
        PythonOperator(
            task_id=f"backfill_{title}",
            python_callable=ingestion_service.run_player_games_backfill,
            op_kwargs={
                "title": title,
                "ds": "{{ ds }}",
            },
        )
