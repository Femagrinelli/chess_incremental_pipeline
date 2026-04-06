"""
Silver dbt DAG. Triggered by chess_bronze; triggers chess_gold on success.

Conf (optional, for re-materializing historical months):
  month_key / month_keys / year / years / start_month / end_month
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.models.param import Param
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_func

import sys
sys.path.insert(0, "/opt/airflow/scripts")
import pipeline_time

DEFAULT_ARGS = {
    "owner": "chess-platform",
    "retries": 1,
    "retry_delay": timedelta(seconds=180),
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


def _build_silver(ds: str) -> None:
    import dbt_runner
    month_keys = _resolve_month_keys(ds)
    if month_keys:
        dbt_runner.build_silver(ds, month_keys=month_keys)
    else:
        dbt_runner.build_silver(ds)


with DAG(
    dag_id="chess_silver",
    description="dbt silver transformations",
    start_date=pipeline_time.airflow_start_date(),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params=MONTH_PARAMS,
    tags=["chess", "silver", "dbt"],
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    build_silver = PythonOperator(
        task_id="build_silver",
        python_callable=_build_silver,
        op_kwargs={"ds": "{{ ds }}"},
    )

    def _trigger_gold(**context) -> None:
        dag_run = context.get("dag_run")
        conf = dict(dag_run.conf) if dag_run and dag_run.conf else {}
        trigger_dag_func(dag_id="chess_gold", conf=conf)

    trigger_gold = PythonOperator(
        task_id="trigger_chess_gold",
        python_callable=_trigger_gold,
    )

    build_silver >> trigger_gold
