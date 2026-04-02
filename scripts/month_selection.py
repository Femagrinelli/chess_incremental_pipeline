"""
Shared month-selection helpers for bronze backfills and dbt model runs.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime

import chess_client
import config
import state_store

logger = logging.getLogger(__name__)

MONTH_KEY_PATTERN = re.compile(r"^\d{4}-\d{2}$")
YEAR_PATTERN = re.compile(r"^\d{4}$")


def validate_month_key(month_key: str) -> str:
    if not isinstance(month_key, str) or not MONTH_KEY_PATTERN.match(month_key):
        raise ValueError(f"Invalid month_key '{month_key}'. Expected YYYY-MM.")

    year, month = month_key.split("-")
    if not 1 <= int(month) <= 12:
        raise ValueError(f"Invalid month_key '{month_key}'. Month must be between 01 and 12.")
    return month_key


def validate_year(year: str) -> str:
    year_str = str(year)
    if not YEAR_PATTERN.match(year_str):
        raise ValueError(f"Invalid year '{year}'. Expected YYYY.")
    return year_str


def parse_string_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    raise ValueError(f"Expected string or list, got {type(value).__name__}.")


def month_range(start_month: str, end_month: str) -> list[str]:
    start_value = validate_month_key(start_month)
    end_value = validate_month_key(end_month)

    start_dt = datetime.strptime(f"{start_value}-01", "%Y-%m-%d")
    end_dt = datetime.strptime(f"{end_value}-01", "%Y-%m-%d")
    if start_dt > end_dt:
        raise ValueError("start_month must be earlier than or equal to end_month.")

    month_keys = []
    current = start_dt
    while current <= end_dt:
        month_keys.append(current.strftime("%Y-%m"))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return month_keys


def available_month_keys_from_state() -> list[str]:
    month_keys: set[str] = set()
    seen_usernames: set[str] = set()

    for title in chess_client.VALID_TITLES:
        state = state_store.load_title_state(title)
        for username, player in state.get("players", {}).items():
            if username in seen_usernames:
                continue
            seen_usernames.add(username)

            current_month_key = player.get("current_month_key")
            if current_month_key:
                month_keys.add(validate_month_key(current_month_key))

            player_index = state_store.load_player_index(username)
            for month_key in player_index.get("stored_months", []):
                month_keys.add(validate_month_key(month_key))

    return sorted(month_keys)


def resolve_month_keys_from_conf(conf: dict | None, ds: str) -> list[str]:
    conf = conf or {}
    month_keys: set[str] = set()

    for month_key in parse_string_list(conf.get("month_keys")):
        month_keys.add(validate_month_key(month_key))

    if conf.get("month_key"):
        month_keys.add(validate_month_key(str(conf["month_key"])))

    years = {
        validate_year(year)
        for year in parse_string_list(conf.get("years"))
    }
    if conf.get("year"):
        years.add(validate_year(conf["year"]))

    start_month = conf.get("start_month")
    end_month = conf.get("end_month")
    if start_month or end_month:
        if not start_month or not end_month:
            raise ValueError("start_month and end_month must be provided together.")
        month_keys.update(month_range(str(start_month), str(end_month)))

    if years:
        matched_months = [
            month_key
            for month_key in available_month_keys_from_state()
            if month_key[:4] in years
        ]
        if not matched_months:
            logger.warning("No month_keys found in state for years=%s", sorted(years))
        month_keys.update(matched_months)

    if not month_keys:
        month_keys.add(validate_month_key(config.current_month_key(ds)))

    resolved = sorted(month_keys)
    logger.info("Resolved month selection | conf=%s | months=%s", conf, resolved)
    return resolved
