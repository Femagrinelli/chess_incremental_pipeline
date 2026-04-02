"""
Helpers to run dbt against the medallion models.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess

import config
import month_selection

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = "/opt/airflow/dbt/chess_medallion"
DBT_PROFILES_DIR = "/opt/airflow/dbt/profiles"


def _dbt_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("DBT_PROFILES_DIR", DBT_PROFILES_DIR)
    return env


def _run_dbt(args: list[str], vars_dict: dict | None = None) -> None:
    cmd = ["dbt", *args, "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROFILES_DIR]
    if vars_dict:
        cmd.extend(["--vars", json.dumps(vars_dict, separators=(",", ":"))])

    logger.info("Running dbt command: %s", " ".join(cmd))
    subprocess.run(cmd, check=True, env=_dbt_env())


def _register_bronze_sources(month_keys: list[str], snapshot_date: str | None = None) -> None:
    vars_dict = {
        "month_keys": month_keys,
        "snapshot_date": snapshot_date,
    }
    _run_dbt(["run-operation", "register_bronze_external_tables"], vars_dict=vars_dict)


def build_silver_current(ds: str) -> None:
    month_keys = [config.current_month_key(ds)]
    _register_bronze_sources(month_keys=month_keys, snapshot_date=ds)
    _run_dbt(
        [
            "build",
            "--select",
            "+silver_roster_daily",
            "+silver_games_core",
            "+silver_player_game_facts",
        ],
        vars_dict={"month_keys": month_keys, "snapshot_date": ds},
    )


def build_gold_current(ds: str) -> None:
    month_keys = [config.current_month_key(ds)]
    _register_bronze_sources(month_keys=month_keys, snapshot_date=ds)
    _run_dbt(
        [
            "build",
            "--select",
            "+gold_title_month_activity",
            "+gold_title_month_rating_volatility",
            "+gold_title_month_color_performance",
        ],
        vars_dict={"month_keys": month_keys, "snapshot_date": ds},
    )


def build_silver_selected(month_keys: list[str], snapshot_date: str | None = None) -> None:
    _register_bronze_sources(month_keys=month_keys, snapshot_date=snapshot_date)
    _run_dbt(
        [
            "build",
            "--select",
            "+silver_games_core",
            "+silver_player_game_facts",
        ],
        vars_dict={"month_keys": month_keys, "snapshot_date": snapshot_date},
    )


def build_gold_selected(month_keys: list[str], snapshot_date: str | None = None) -> None:
    _register_bronze_sources(month_keys=month_keys, snapshot_date=snapshot_date)
    _run_dbt(
        [
            "build",
            "--select",
            "+gold_title_month_activity",
            "+gold_title_month_rating_volatility",
            "+gold_title_month_color_performance",
        ],
        vars_dict={"month_keys": month_keys, "snapshot_date": snapshot_date},
    )


def resolve_month_keys_from_conf(conf: dict | None, ds: str) -> list[str]:
    return month_selection.resolve_month_keys_from_conf(conf, ds)
