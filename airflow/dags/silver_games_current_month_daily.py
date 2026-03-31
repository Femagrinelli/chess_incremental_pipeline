from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow/scripts")

TITLES = ("GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM")


def _materialize_current_month_title(title: str, ds: str) -> None:
    import silver_service

    silver_service.materialize_current_month_title(title=title, ds=ds)


DEFAULT_ARGS = {
    "owner": "chess-platform",
    "retries": 1,
    "retry_delay": timedelta(seconds=180),
}


with DAG(
    dag_id="silver_games_current_month_daily",
    description="Materialize current-month titled-player silver datasets by title",
    start_date=datetime(2024, 1, 1),
    schedule_interval="15 4 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["chess", "silver", "games", "current-month"],
    max_active_runs=1,
    max_active_tasks=1,
) as dag:
    tasks = []
    for title in TITLES:
        tasks.append(
            PythonOperator(
                task_id=f"materialize_current_month_silver_{title}",
                python_callable=_materialize_current_month_title,
                op_kwargs={
                    "title": title,
                    "ds": "{{ ds }}",
                },
            )
        )

    for upstream, downstream in zip(tasks, tasks[1:]):
        upstream >> downstream
