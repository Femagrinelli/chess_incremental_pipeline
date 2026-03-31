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
    "retry_delay": timedelta(seconds=180),
}


def _materialize_months_from_conf(ds: str, **_kwargs) -> None:
    import gold_service

    context = get_current_context()
    dag_run = context.get("dag_run")
    dag_run_conf = dag_run.conf if dag_run and dag_run.conf else {}
    params = dict(context.get("params") or {})
    conf = {**params, **dag_run_conf}
    month_keys = gold_service.resolve_month_keys_from_conf(conf, ds)
    gold_service.materialize_selected_months(month_keys)


with DAG(
    dag_id="gold_title_month_backfill",
    description="Materialize selected historical months or years into gold title-month marts",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "month_key": Param(
            default=None,
            type=["null", "string"],
            title="Single Month",
            description="One month in YYYY-MM format.",
        ),
        "month_keys": Param(
            default=[],
            type="array",
            items={"type": "string"},
            title="Month List",
            description="Optional list of months such as ['2026-01', '2026-02'].",
        ),
        "year": Param(
            default=None,
            type=["null", "string"],
            title="Single Year",
            description="One year in YYYY format.",
        ),
        "years": Param(
            default=[],
            type="array",
            items={"type": "string"},
            title="Year List",
            description="Optional list of years such as ['2025', '2026'].",
        ),
        "start_month": Param(
            default=None,
            type=["null", "string"],
            title="Start Month",
            description="Inclusive start month in YYYY-MM format when using a range.",
        ),
        "end_month": Param(
            default=None,
            type=["null", "string"],
            title="End Month",
            description="Inclusive end month in YYYY-MM format when using a range.",
        ),
    },
    tags=["chess", "gold", "metrics", "backfill"],
    max_active_runs=1,
    max_active_tasks=1,
) as dag:
    PythonOperator(
        task_id="materialize_gold_months",
        python_callable=_materialize_months_from_conf,
        op_kwargs={
            "ds": "{{ ds }}",
        },
    )
