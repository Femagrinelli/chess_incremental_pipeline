from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow/scripts")

import chess_client
import ingestion_service


DEFAULT_ARGS = {
    "owner": "chess-platform",
    "retries": 2,
    "retry_delay": timedelta(seconds=120),
}


with DAG(
    dag_id="player_games_current_month_daily",
    description="Sync only the current month for players marked active this month",
    start_date=datetime(2024, 1, 1),
    schedule_interval="10 1 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["chess", "bronze", "games", "current-month"],
    max_active_runs=1,
    max_active_tasks=2,
) as dag:
    for title in chess_client.VALID_TITLES:
        PythonOperator(
            task_id=f"current_month_{title}",
            python_callable=ingestion_service.run_player_games_current_sync,
            op_kwargs={
                "title": title,
                "ds": "{{ ds }}",
            },
        )
