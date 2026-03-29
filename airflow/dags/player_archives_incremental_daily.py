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
    dag_id="player_archives_incremental_daily",
    description="Refresh archives only for new, active or stale players",
    start_date=datetime(2024, 1, 1),
    schedule_interval="35 0 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["chess", "bronze", "archives"],
    max_active_runs=1,
    max_active_tasks=2,
) as dag:
    for title in chess_client.VALID_TITLES:
        PythonOperator(
            task_id=f"archives_{title}",
            python_callable=ingestion_service.run_player_archives_refresh,
            op_kwargs={
                "title": title,
                "ds": "{{ ds }}",
            },
        )
