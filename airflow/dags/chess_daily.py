"""
Daily ingestion DAG: snapshot rosters, refresh archives, sync current-month
games. Triggers chess_bronze on success. Historical backfill lives in its own
DAG (chess_backfill).
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_func

import sys
sys.path.insert(0, "/opt/airflow/scripts")
import pipeline_time

TITLES = ("GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM")

DEFAULT_ARGS = {
    "owner": "chess-platform",
    "retries": 3,
    "retry_delay": timedelta(seconds=60),
}


def _snapshot_rosters(ds: str) -> None:
    import ingestion_service
    for title in TITLES:
        ingestion_service.run_titled_players_snapshot(title=title, ds=ds)


def _refresh_archives(ds: str) -> None:
    import ingestion_service
    for title in TITLES:
        ingestion_service.run_player_archives_refresh(title=title, ds=ds)


def _sync_current_month_games(ds: str) -> None:
    import ingestion_service
    for title in TITLES:
        ingestion_service.run_player_games_current_sync(title=title, ds=ds)


with DAG(
    dag_id="chess_daily",
    description="Daily ingestion: roster -> archives -> current-month games",
    start_date=pipeline_time.airflow_start_date(),
    schedule_interval="0 0 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["chess", "ingest"],
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    snapshot_rosters = PythonOperator(
        task_id="snapshot_rosters",
        python_callable=_snapshot_rosters,
        op_kwargs={"ds": "{{ ds }}"},
        pool="chess_api",
    )

    refresh_archives = PythonOperator(
        task_id="refresh_archives",
        python_callable=_refresh_archives,
        op_kwargs={"ds": "{{ ds }}"},
        pool="chess_api",
    )

    sync_current_month_games = PythonOperator(
        task_id="sync_current_month_games",
        python_callable=_sync_current_month_games,
        op_kwargs={"ds": "{{ ds }}"},
        pool="chess_api",
    )

    def _trigger_bronze(**context) -> None:
        trigger_dag_func(dag_id="chess_bronze", conf={"ds": context["ds"]})

    trigger_bronze = PythonOperator(
        task_id="trigger_chess_bronze",
        python_callable=_trigger_bronze,
    )

    snapshot_rosters >> refresh_archives >> sync_current_month_games >> trigger_bronze
