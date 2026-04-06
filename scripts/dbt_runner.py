"""
Helpers to run dbt against the medallion models.
"""

from __future__ import annotations

import logging
import os
import subprocess
import tempfile

import config
import month_selection

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = "/opt/airflow/dbt/chess_medallion"
DBT_PROFILES_DIR = "/opt/airflow/dbt/profiles"
DBT_TARGET_PATH = "/home/airflow/dbt-target"
DBT_LOG_PATH = "/home/airflow/dbt-logs"
DBT_RUNTIME_DIR = "/home/airflow/dbt-runtime"


def _vars_arg(values: dict) -> list[str]:
    compact = ",".join(f'"{key}":"{value}"' for key, value in values.items() if value is not None)
    return ["--vars", "{" + compact + "}"] if compact else []


def _run_dbt(command: list[str], vars_dict: dict | None = None) -> None:
    os.makedirs(DBT_TARGET_PATH, exist_ok=True)
    os.makedirs(DBT_LOG_PATH, exist_ok=True)
    os.makedirs(DBT_RUNTIME_DIR, exist_ok=True)

    fd, db_path = tempfile.mkstemp(
        prefix="chess_pipeline_",
        suffix=".duckdb",
        dir=DBT_RUNTIME_DIR,
    )
    os.close(fd)
    os.remove(db_path)

    cmd = [
        "dbt",
        *command,
        "--project-dir",
        DBT_PROJECT_DIR,
        "--profiles-dir",
        DBT_PROFILES_DIR,
        "--target-path",
        DBT_TARGET_PATH,
        "--log-path",
        DBT_LOG_PATH,
    ]
    if vars_dict:
        cmd.extend(_vars_arg(vars_dict))

    env = os.environ.copy()
    env["DBT_DUCKDB_PATH"] = db_path
    logger.info("Running dbt command: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True, env=env)
    finally:
        for path in (db_path, f"{db_path}.wal"):
            try:
                os.remove(path)
            except FileNotFoundError:
                pass


def _build_silver_month(month_key: str) -> None:
    _run_dbt(
        [
            "build",
            "--select",
            "silver_games_core",
            "silver_player_game_facts",
        ],
        vars_dict={"month_key": month_key},
    )


def _build_gold_month(month_key: str) -> None:
    _run_dbt(
        [
            "build",
            "--select",
            "gold_title_month_activity",
            "gold_title_month_rating_volatility",
            "gold_title_month_color_performance",
            "gold_head_to_head_games",
            "gold_head_to_head_monthly",
        ],
        vars_dict={"month_key": month_key},
    )


def _build_head_to_head_summary() -> None:
    _run_dbt(["build", "--select", "gold_head_to_head_summary"])


def build_silver(ds: str, month_keys: list[str] | None = None) -> None:
    if month_keys is None:
        month_keys = [config.current_month_key(ds)]
        _run_dbt(
            ["build", "--select", "silver_roster_daily"],
            vars_dict={"snapshot_date": ds},
        )
    for month_key in month_keys:
        _build_silver_month(month_key)


def build_gold(ds: str, month_keys: list[str] | None = None) -> None:
    if month_keys is None:
        month_keys = [config.current_month_key(ds)]
    for month_key in month_keys:
        _build_gold_month(month_key)
    _build_head_to_head_summary()


def resolve_month_keys_from_conf(conf: dict | None, ds: str) -> list[str]:
    return month_selection.resolve_month_keys_from_conf(conf, ds)
