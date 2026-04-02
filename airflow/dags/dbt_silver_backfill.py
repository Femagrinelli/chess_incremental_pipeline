from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context

import sys
sys.path.insert(0, "/opt/airflow/scripts")


DEFAULT_ARGS = {
    "owner": "chess-platform",
    "retries": 1,
    "retry_delay": timedelta(seconds=300),
}


def _build_silver_from_conf(ds: str, **_kwargs) -> None:
    import dbt_runner

    context = get_current_context()
    dag_run = context.get("dag_run")
    dag_run_conf = dag_run.conf if dag_run and dag_run.conf else {}
    params = dict(context.get("params") or {})
    conf = {**params, **dag_run_conf}
    month_keys = dbt_runner.resolve_month_keys_from_conf(conf, ds)
    dbt_runner.build_silver_selected(month_keys=month_keys)


with DAG(
    dag_id="dbt_silver_backfill",
    description="Build selected historical silver models in dbt from bronze parquet",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "month_key": Param(default=None, type=["null", "string"], title="Single Month"),
        "month_keys": Param(default=[], type="array", items={"type": "string"}, title="Month List"),
        "year": Param(default=None, type=["null", "string"], title="Single Year"),
        "years": Param(default=[], type="array", items={"type": "string"}, title="Year List"),
        "start_month": Param(default=None, type=["null", "string"], title="Start Month"),
        "end_month": Param(default=None, type=["null", "string"], title="End Month"),
    },
    tags=["chess", "dbt", "silver", "backfill"],
    max_active_runs=1,
    max_active_tasks=1,
) as dag:
    PythonOperator(
        task_id="build_silver_models",
        python_callable=_build_silver_from_conf,
        op_kwargs={
            "ds": "{{ ds }}",
        },
    )
