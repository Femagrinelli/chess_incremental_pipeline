"""
Helpers to manage the hot title manifest and the cold per-player index.
"""

import copy

import config
import storage_client


def build_empty_title_state(title: str) -> dict:
    return {
        "title": title,
        "latest_snapshot_date": None,
        "last_snapshot_key": None,
        "last_updated_at": None,
        "players": {},
    }


def build_empty_player_state(username: str, title: str) -> dict:
    return {
        "username": username,
        "title": title,
        "status": "active",
        "first_seen_snapshot_date": None,
        "last_seen_snapshot_date": None,
        "removed_snapshot_date": None,
        "archive_last_checked_at": None,
        "archive_hash": None,
        "archive_snapshot_key": None,
        "latest_archive_month": None,
        "has_current_month_archive": False,
        "current_month_key": None,
        "current_month_etag": None,
        "current_month_last_modified": None,
        "current_month_game_key": None,
        "current_month_game_count": 0,
        "last_game_sync_at": None,
        "backfill_pending": False,
        "backfill_pending_count": 0,
        "player_index_key": config.player_index_key(username),
    }


def load_title_state(title: str) -> dict:
    state = storage_client.download_json(config.title_state_key(title))
    if state:
        return state
    return build_empty_title_state(title)


def save_title_state(title: str, state: dict) -> None:
    payload = copy.deepcopy(state)
    payload["last_updated_at"] = config.utc_now_iso()
    storage_client.upload_json(config.title_state_key(title), payload)


def build_empty_player_index(username: str) -> dict:
    return {
        "username": username,
        "stored_months": [],
        "archive_months": [],
        "pending_backfill_months": [],
        "latest_archive_snapshot_key": None,
        "updated_at": None,
    }


def load_player_index(username: str) -> dict:
    index = storage_client.download_json(config.player_index_key(username))
    if index:
        return index
    return build_empty_player_index(username)


def save_player_index(username: str, index: dict) -> None:
    payload = copy.deepcopy(index)
    payload["updated_at"] = config.utc_now_iso()
    storage_client.upload_json(config.player_index_key(username), payload)
