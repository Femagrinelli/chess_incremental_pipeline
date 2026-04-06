"""
One-time migration DAG: rewrite raw/player_games keys from the legacy layout
(raw/player_games/{username}/{year}/{month}.json) to the new layout
(raw/player_games/year=/month=/username=.json).

Sharded by a username prefix passed in at trigger time so the full ~900k
players can be split across multiple runs. Examples:

  # single shard
  airflow dags trigger chess_migrate_raw_layout --conf '{"prefix":"ma"}'

  # multiple shards in one run
  airflow dags trigger chess_migrate_raw_layout --conf '{"prefixes":["a","b","c"]}'

  # everything in one go (not recommended for large buckets)
  airflow dags trigger chess_migrate_raw_layout --conf '{"prefix":""}'

  # dry-run first
  airflow dags trigger chess_migrate_raw_layout --conf '{"prefix":"a","dry_run":true}'

max_active_runs=4, so up to four prefix-shards can be run in parallel by
triggering the DAG repeatedly with different prefixes.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.models.param import Param

import sys
sys.path.insert(0, "/opt/airflow/scripts")
import pipeline_time

DEFAULT_ARGS = {
    "owner": "chess-platform",
    "retries": 1,
    "retry_delay": timedelta(seconds=120),
}

PARAMS = {
    "prefix": Param(
        default="",
        type=["null", "string"],  # allows null + string
        minLength=0,
        title="Username prefix",
        description=(
            "Single S3 list prefix applied to raw/player_games/. "
            "Examples: 'a' (all usernames starting with a), 'ma' (narrower), "
            "'' (empty = everything). Leave blank if you are using 'prefixes' instead."
        ),
    ),
    "prefixes": Param(
        default=[],
        type="array",
        items={"type": "string"},
        title="Prefix list",
        description=(
            "Alternative to 'prefix': run multiple shards serially in one DAG run. "
            "Example: [\"a\",\"b\",\"c\"]. Ignored if empty."
        ),
    ),
    "dry_run": Param(
        default=False,
        type="boolean",
        title="Dry run",
        description="If true, logs the plan and sample moves without touching S3.",
    ),
    "max_workers": Param(
        default=16,
        type="integer",
        minimum=1,
        maximum=64,
        title="Parallel move workers",
        description="Thread pool size for server-side S3 copy+delete operations.",
    ),
}


def _resolve_conf() -> dict:
    context = get_current_context()
    dag_run = context.get("dag_run")
    dag_run_conf = dag_run.conf if dag_run and dag_run.conf else {}
    params = dict(context.get("params") or {})
    return {**params, **dag_run_conf}


def _resolve_prefixes(conf: dict) -> list[str]:
    prefixes = list(conf.get("prefixes") or [])
    # `prefix` defaults to "" in the UI form; only treat it as supplied if the
    # user didn't also pass a non-empty `prefixes` list.
    single = conf.get("prefix")
    if not prefixes and single is not None:
        prefixes.append(single)
    # Default to "" (all usernames) when nothing is specified.
    if not prefixes:
        prefixes = [""]
    # preserve order, de-dupe
    seen: set[str] = set()
    deduped: list[str] = []
    for p in prefixes:
        if p not in seen:
            seen.add(p)
            deduped.append(p)
    return deduped


def _run_migration() -> None:
    import migrate_raw_layout
    conf = _resolve_conf()
    dry_run = bool(conf.get("dry_run", False))
    max_workers = int(conf.get("max_workers", 16))

    for prefix in _resolve_prefixes(conf):
        migrate_raw_layout.migrate_prefix(
            prefix=prefix,
            dry_run=dry_run,
            max_workers=max_workers,
        )


with DAG(
    dag_id="chess_migrate_raw_layout",
    description="One-time: migrate legacy raw/player_games keys to year=/month=/username= layout",
    start_date=pipeline_time.airflow_start_date(),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params=PARAMS,
    tags=["chess", "migration"],
    max_active_runs=4,
    max_active_tasks=1,
) as dag:

    PythonOperator(
        task_id="migrate_raw_layout",
        python_callable=_run_migration,
    )
