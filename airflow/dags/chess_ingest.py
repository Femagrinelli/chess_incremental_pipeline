"""
Daily ingestion DAG: snapshot rosters, refresh archives, sync current-month
games, and drain backfill queues.  Triggers chess_bronze on success.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

import sys
sys.path.insert(0, "/opt/airflow/scripts")
import pipeline_time

TITLES = ("GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM")

DEFAULT_ARGS = {
    "owner": "chess-platform",
    "retries": 3,
    "retry_delay": timedelta(seconds=60),
}


def _snapshot_titled_players(title: str, ds: str) -> None:
    import ingestion_service
    ingestion_service.run_titled_players_snapshot(title=title, ds=ds)


def _refresh_archives(title: str, ds: str) -> None:
    import ingestion_service
    ingestion_service.run_player_archives_refresh(title=title, ds=ds)


def _sync_current_month_games(title: str, ds: str) -> None:
    import ingestion_service
    ingestion_service.run_player_games_current_sync(title=title, ds=ds)


def _backfill_games(title: str, ds: str) -> None:
    import ingestion_service
    ingestion_service.run_player_games_backfill(title=title, ds=ds)


with DAG(
    dag_id="chess_ingest",
    description="Daily ingestion: roster -> archives -> games -> backfill",
    start_date=pipeline_time.airflow_start_date(),
    schedule_interval="0 0 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["chess", "ingest"],
    max_active_runs=1,
    max_active_tasks=4,
) as dag:

    with TaskGroup("ingest_roster") as tg_roster:
        for title in TITLES:
            PythonOperator(
                task_id=f"snapshot_{title}",
                python_callable=_snapshot_titled_players,
                op_kwargs={"title": title, "ds": "{{ ds }}"},
                pool="chess_api",
            )

    with TaskGroup("ingest_archives") as tg_archives:
        for title in TITLES:
            PythonOperator(
                task_id=f"archives_{title}",
                python_callable=_refresh_archives,
                op_kwargs={"title": title, "ds": "{{ ds }}"},
                pool="chess_api",
            )

    with TaskGroup("ingest_games") as tg_games:
        for title in TITLES:
            PythonOperator(
                task_id=f"current_month_{title}",
                python_callable=_sync_current_month_games,
                op_kwargs={"title": title, "ds": "{{ ds }}"},
                pool="chess_api",
            )

    with TaskGroup("ingest_backfill") as tg_backfill:
        for title in TITLES:
            PythonOperator(
                task_id=f"backfill_{title}",
                python_callable=_backfill_games,
                op_kwargs={"title": title, "ds": "{{ ds }}"},
                pool="chess_api",
            )

    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_chess_bronze",
        trigger_dag_id="chess_bronze",
        conf={"ds": "{{ ds }}"},
        wait_for_completion=False,
    )

    tg_roster >> tg_archives >> tg_games >> tg_backfill >> trigger_bronze
