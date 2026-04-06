"""
Bronze materialization DAG: roster snapshots + player games -> parquet.
Triggered by chess_daily; triggers chess_silver.

Conf:
  month_key / month_keys / year / years / start_month / end_month to
  re-materialize arbitrary historical months. When no month conf is given,
  materializes the current month (default daily path).
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.models.param import Param
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_func

import sys
sys.path.insert(0, "/opt/airflow/scripts")
import pipeline_time

TITLES = ("GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM")

DEFAULT_ARGS = {
    "owner": "chess-platform",
    "retries": 2,
    "retry_delay": timedelta(seconds=120),
}

MONTH_PARAMS = {
    "month_key": Param(default=None, type=["null", "string"], title="Single Month"),
    "month_keys": Param(default=[], type="array", items={"type": "string"}, title="Month List"),
    "year": Param(default=None, type=["null", "string"], title="Single Year"),
    "years": Param(default=[], type="array", items={"type": "string"}, title="Year List"),
    "start_month": Param(default=None, type=["null", "string"], title="Start Month"),
    "end_month": Param(default=None, type=["null", "string"], title="End Month"),
}


def _resolve_month_keys(ds: str) -> list[str]:
    import bronze_service
    context = get_current_context()
    dag_run = context.get("dag_run")
    dag_run_conf = dag_run.conf if dag_run and dag_run.conf else {}
    params = dict(context.get("params") or {})
    conf = {**params, **dag_run_conf}
    return bronze_service.resolve_month_keys_from_conf(conf, ds)


def _materialize_rosters(ds: str) -> None:
    import bronze_service
    for title in TITLES:
        bronze_service.materialize_title_roster_snapshot(title=title, ds=ds)


def _materialize_games(ds: str) -> None:
    import bronze_service
    month_keys = _resolve_month_keys(ds)
    if month_keys:
        bronze_service.materialize_selected_months(month_keys)
    else:
        bronze_service.materialize_current_month(ds)


with DAG(
    dag_id="chess_bronze",
    description="Bronze materialization: roster + games -> parquet",
    start_date=pipeline_time.airflow_start_date(),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params=MONTH_PARAMS,
    tags=["chess", "bronze"],
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    materialize_rosters = PythonOperator(
        task_id="materialize_rosters",
        python_callable=_materialize_rosters,
        op_kwargs={"ds": "{{ ds }}"},
    )

    materialize_games = PythonOperator(
        task_id="materialize_games",
        python_callable=_materialize_games,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # def _trigger_silver(**context) -> None:
    #     dag_run = context.get("dag_run")
    #     conf = dict(dag_run.conf) if dag_run and dag_run.conf else {}
    #     trigger_dag_func(dag_id="chess_silver", conf=conf)

    # trigger_silver = PythonOperator(
    #     task_id="trigger_chess_silver",
    #     python_callable=_trigger_silver,
    # )

    materialize_rosters >> materialize_games
    # >> trigger_silver
