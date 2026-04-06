"""
Historical-games backfill DAG: drain each title's queued backfill months from
the Chess.com API into raw. Manually triggered when backfill work is needed.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow/scripts")
import pipeline_time

TITLES = ("GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM")

DEFAULT_ARGS = {
    "owner": "chess-platform",
    "retries": 2,
    "retry_delay": timedelta(seconds=120),
}


def _backfill_games(ds: str) -> None:
    import ingestion_service
    for title in TITLES:
        ingestion_service.run_player_games_backfill(title=title, ds=ds)


with DAG(
    dag_id="chess_backfill",
    description="Drain queued historical-month backfill for all titles",
    start_date=pipeline_time.airflow_start_date(),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["chess", "backfill"],
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    PythonOperator(
        task_id="backfill_games",
        python_callable=_backfill_games,
        op_kwargs={"ds": "{{ ds }}"},
        pool="chess_api",
    )
