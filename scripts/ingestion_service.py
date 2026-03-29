"""
Core orchestration logic shared by the DAGs.
"""

import hashlib
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import chess_client
import config
import state_store
import storage_client

logger = logging.getLogger(__name__)

ARCHIVE_DAILY_KEY_PATTERN = re.compile(r"^.*/(?P<date>\d{4}-\d{2}-\d{2})\.json$")
ARCHIVE_MONTHLY_KEY_PATTERN = re.compile(r"^.*/(?P<year>\d{4})/(?P<month>\d{2})\.json$")
GAME_KEY_PATTERN = re.compile(r"^.*/(?P<year>\d{4})/(?P<month>\d{2})\.json$")


def _hash_value(value) -> str:
    payload = json.dumps(value, sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def _parse_date_to_iso_start(date_str: str) -> str:
    return f"{date_str}T00:00:00+00:00"


def _archive_key_sort_value(key: str) -> tuple[int, str]:
    daily_match = ARCHIVE_DAILY_KEY_PATTERN.match(key)
    if daily_match:
        return 2, daily_match.group("date")

    monthly_match = ARCHIVE_MONTHLY_KEY_PATTERN.match(key)
    if monthly_match:
        return 1, f"{monthly_match.group('year')}-{monthly_match.group('month')}"

    return 0, key


def _latest_archive_key(username: str) -> str | None:
    keys = storage_client.list_objects(config.player_archives_prefix(username))
    if not keys:
        return None
    return max(keys, key=_archive_key_sort_value)


def _stored_game_months(username: str) -> list[str]:
    keys = storage_client.list_objects(config.player_games_prefix(username))
    month_keys = []
    for key in keys:
        match = GAME_KEY_PATTERN.match(key)
        if match:
            month_keys.append(f"{match.group('year')}-{match.group('month')}")
    return sorted(set(month_keys))


def _latest_title_snapshot_key(title: str) -> str | None:
    keys = storage_client.list_objects(config.titled_players_prefix(title))
    if not keys:
        return None
    return max(keys)


def run_titled_players_snapshot(title: str, ds: str) -> None:
    snapshot = chess_client.get_titled_players(title)
    players = sorted(set(snapshot.get("players", [])))
    snapshot_key = config.titled_players_snapshot_key(title, ds)

    storage_client.upload_json(
        snapshot_key,
        {
            "title": title,
            "snapshot_date": ds,
            "player_count": len(players),
            "players": players,
        },
    )

    state = state_store.load_title_state(title)
    state["latest_snapshot_date"] = ds
    state["last_snapshot_key"] = snapshot_key

    previous_active = {
        username
        for username, player in state["players"].items()
        if player.get("status") == "active"
    }
    current_players = set(players)

    new_players = current_players - previous_active
    removed_players = previous_active - current_players

    for username in players:
        player = state["players"].get(username) or state_store.build_empty_player_state(username, title)
        player["status"] = "active"
        player["first_seen_snapshot_date"] = player.get("first_seen_snapshot_date") or ds
        player["last_seen_snapshot_date"] = ds
        player["removed_snapshot_date"] = None
        state["players"][username] = player

    for username in removed_players:
        player = state["players"][username]
        player["status"] = "removed"
        player["removed_snapshot_date"] = ds

    state_store.save_title_state(title, state)
    logger.info(
        "[%s] titled players snapshot saved | total=%d | new=%d | removed=%d",
        title,
        len(players),
        len(new_players),
        len(removed_players),
    )


def _should_refresh_archives(player: dict, now: datetime) -> bool:
    if player.get("status") != "active":
        return False

    last_checked = _parse_iso(player.get("archive_last_checked_at"))
    if last_checked is None:
        return True

    if player.get("has_current_month_archive"):
        return now - last_checked >= timedelta(hours=config.ACTIVE_ARCHIVE_REFRESH_HOURS)

    return now - last_checked >= timedelta(days=config.ARCHIVE_REFRESH_DAYS)


def _refresh_archives_for_player(title: str, username: str, current_month: str) -> dict:
    checked_at = config.utc_now_iso()
    archives_response = chess_client.get_player_archives(username)
    archive_urls = sorted(set(archives_response.get("archives", [])))
    archive_months = chess_client.extract_archive_months(archive_urls)
    archive_hash = _hash_value(archive_months)
    snapshot_key = None
    pending_backfill_months = None

    return {
        "username": username,
        "title": title,
        "checked_at": checked_at,
        "archive_urls": archive_urls,
        "archive_months": archive_months,
        "archive_hash": archive_hash,
        "latest_archive_month": archive_months[-1] if archive_months else None,
        "has_current_month_archive": current_month in archive_months,
        "archive_snapshot_key": snapshot_key,
        "pending_backfill_months": pending_backfill_months,
    }


def run_player_archives_refresh(title: str, ds: str) -> None:
    state = state_store.load_title_state(title)
    players = state.get("players", {})
    if not players:
        logger.warning("[%s] no players found in title state", title)
        return

    current_month = config.current_month_key(ds)
    now = config.utc_now()
    usernames = [
        username
        for username, player in players.items()
        if _should_refresh_archives(player, now)
    ]

    if not usernames:
        logger.info("[%s] no players need archive refresh", title)
        return

    logger.info("[%s] refreshing archives for %d players", title, len(usernames))

    refreshed = 0
    changed = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=config.TITLE_TASK_MAX_WORKERS) as executor:
        futures = {
            executor.submit(_refresh_archives_for_player, title, username, current_month): username
            for username in usernames
        }

        for future in as_completed(futures):
            username = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                logger.error("[%s] archive refresh failed for %s: %s", title, username, exc)
                errors += 1
                continue

            player = players[username]
            refreshed += 1

            archive_changed = result["archive_hash"] != player.get("archive_hash")
            snapshot_key = player.get("archive_snapshot_key")
            pending_count = player.get("backfill_pending_count", 0)

            if archive_changed:
                snapshot_key = config.player_archives_snapshot_key(username, ds)
                storage_client.upload_json(
                    snapshot_key,
                    {
                        "title": title,
                        "username": username,
                        "snapshot_date": ds,
                        "archive_count": len(result["archive_months"]),
                        "archives": result["archive_urls"],
                    },
                )

                player_index = state_store.load_player_index(username)
                stored_months = set(player_index.get("stored_months", []))
                pending_backfill_months = [
                    month_key
                    for month_key in result["archive_months"]
                    if month_key not in stored_months and month_key != current_month
                ]

                player_index["archive_months"] = result["archive_months"]
                player_index["latest_archive_snapshot_key"] = snapshot_key
                player_index["pending_backfill_months"] = pending_backfill_months
                state_store.save_player_index(username, player_index)

                pending_count = len(pending_backfill_months)
                changed += 1

            player["archive_last_checked_at"] = result["checked_at"]
            player["archive_hash"] = result["archive_hash"]
            player["archive_snapshot_key"] = snapshot_key
            player["latest_archive_month"] = result["latest_archive_month"]
            player["has_current_month_archive"] = result["has_current_month_archive"]
            player["backfill_pending"] = pending_count > 0
            player["backfill_pending_count"] = pending_count

    state_store.save_title_state(title, state)
    logger.info(
        "[%s] archive refresh done | refreshed=%d | changed=%d | errors=%d",
        title,
        refreshed,
        changed,
        errors,
    )


def _bootstrap_player_state_from_legacy_raw(
    title: str,
    username: str,
    snapshot_date: str,
    current_month: str,
) -> dict:
    player_state = state_store.build_empty_player_state(username, title)
    player_index = state_store.build_empty_player_index(username)

    player_state["first_seen_snapshot_date"] = snapshot_date
    player_state["last_seen_snapshot_date"] = snapshot_date

    archive_key = _latest_archive_key(username)
    archive_months = []
    archive_last_checked_at = config.utc_now_iso()

    if archive_key:
        archive_payload = storage_client.download_json(archive_key) or {}
        archive_months = chess_client.extract_archive_months(archive_payload.get("archives", []))

        daily_match = ARCHIVE_DAILY_KEY_PATTERN.match(archive_key)
        if daily_match:
            archive_last_checked_at = _parse_date_to_iso_start(daily_match.group("date"))

        player_state["archive_hash"] = _hash_value(archive_months)
        player_state["archive_snapshot_key"] = archive_key
        player_state["latest_archive_month"] = archive_months[-1] if archive_months else None
        player_state["has_current_month_archive"] = current_month in archive_months
        player_state["archive_last_checked_at"] = archive_last_checked_at

        player_index["archive_months"] = archive_months
        player_index["latest_archive_snapshot_key"] = archive_key

    stored_months = _stored_game_months(username)
    pending_backfill_months = [
        month_key
        for month_key in archive_months
        if month_key not in stored_months and month_key != current_month
    ]

    player_index["stored_months"] = stored_months
    player_index["pending_backfill_months"] = pending_backfill_months
    state_store.save_player_index(username, player_index)

    if current_month in stored_months:
        year, month = config.split_month_key(current_month)
        player_state["current_month_key"] = current_month
        player_state["current_month_game_key"] = config.player_games_key(username, year, month)
        current_month_payload = storage_client.download_json(player_state["current_month_game_key"])
        if current_month_payload:
            player_state["current_month_game_count"] = current_month_payload.get("game_count", 0)

    player_state["backfill_pending"] = len(pending_backfill_months) > 0
    player_state["backfill_pending_count"] = len(pending_backfill_months)

    return player_state


def bootstrap_title_state_from_legacy_raw(title: str, ds: str) -> None:
    snapshot_key = _latest_title_snapshot_key(title)
    if not snapshot_key:
        logger.warning("[%s] no legacy titled_players snapshot found", title)
        return

    snapshot = storage_client.download_json(snapshot_key)
    if not snapshot:
        logger.warning("[%s] failed to read legacy titled_players snapshot", title)
        return

    snapshot_date = snapshot.get("snapshot_date") or ds
    current_month = config.current_month_key(ds)
    players = sorted(set(snapshot.get("players", [])))

    state = state_store.build_empty_title_state(title)
    state["latest_snapshot_date"] = snapshot_date
    state["last_snapshot_key"] = snapshot_key

    logger.info("[%s] bootstrapping state for %d players from legacy raw", title, len(players))

    errors = 0

    with ThreadPoolExecutor(max_workers=config.TITLE_TASK_MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                _bootstrap_player_state_from_legacy_raw,
                title,
                username,
                snapshot_date,
                current_month,
            ): username
            for username in players
        }

        for future in as_completed(futures):
            username = futures[future]
            try:
                state["players"][username] = future.result()
            except Exception as exc:
                logger.error("[%s] bootstrap failed for %s: %s", title, username, exc)
                errors += 1

    state_store.save_title_state(title, state)
    logger.info(
        "[%s] legacy bootstrap complete | players=%d | errors=%d",
        title,
        len(players),
        errors,
    )


def _finalize_previous_current_month(title: str, state: dict, current_month: str) -> None:
    finalized = 0

    for username, player in state.get("players", {}).items():
        previous_month = player.get("current_month_key")
        previous_game_key = player.get("current_month_game_key")

        if not previous_month or previous_month == current_month or not previous_game_key:
            continue

        player_index = state_store.load_player_index(username)
        stored_months = set(player_index.get("stored_months", []))
        if previous_month not in stored_months:
            stored_months.add(previous_month)
            player_index["stored_months"] = sorted(stored_months)
            state_store.save_player_index(username, player_index)

        player["current_month_key"] = None
        player["current_month_etag"] = None
        player["current_month_last_modified"] = None
        player["current_month_game_key"] = None
        player["current_month_game_count"] = 0
        finalized += 1

    if finalized:
        logger.info("[%s] finalized %d previous current-month files", title, finalized)


def _sync_current_month_for_player(username: str, month_key: str, player: dict) -> dict:
    year, month = config.split_month_key(month_key)
    etag = None
    last_modified = None

    if player.get("current_month_key") == month_key:
        etag = player.get("current_month_etag")
        last_modified = player.get("current_month_last_modified")

    response = chess_client.get_games_for_month_if_changed(
        username=username,
        year=year,
        month=month,
        etag=etag,
        last_modified=last_modified,
    )

    if response.status_code == 304:
        return {
            "username": username,
            "status": "not_modified",
            "checked_at": config.utc_now_iso(),
            "month_key": month_key,
        }

    if response.status_code == 404 or not response.data:
        return {
            "username": username,
            "status": "empty",
            "checked_at": config.utc_now_iso(),
            "month_key": month_key,
        }

    object_key = config.player_games_key(username, year, month)
    games = response.data.get("games", [])
    storage_client.upload_json(
        object_key,
        {
            "username": username,
            "year": year,
            "month": month,
            "month_key": month_key,
            "game_count": len(games),
            "games": games,
        },
        metadata={
            "source-etag": response.etag,
            "source-last-modified": response.last_modified,
        },
    )

    return {
        "username": username,
        "status": "updated",
        "checked_at": config.utc_now_iso(),
        "month_key": month_key,
        "game_count": len(games),
        "object_key": object_key,
        "etag": response.etag,
        "last_modified": response.last_modified,
    }


def run_player_games_current_sync(title: str, ds: str) -> None:
    state = state_store.load_title_state(title)
    players = state.get("players", {})
    if not players:
        logger.warning("[%s] no players found in title state", title)
        return

    month_key = config.current_month_key(ds)
    _finalize_previous_current_month(title, state, month_key)

    usernames = [
        username
        for username, player in players.items()
        if player.get("status") == "active" and player.get("has_current_month_archive")
    ]

    if not usernames:
        state_store.save_title_state(title, state)
        logger.info("[%s] no active players for current month sync", title)
        return

    updated = 0
    not_modified = 0
    empty = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=config.TITLE_TASK_MAX_WORKERS) as executor:
        futures = {
            executor.submit(_sync_current_month_for_player, username, month_key, players[username]): username
            for username in usernames
        }

        for future in as_completed(futures):
            username = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                logger.error("[%s] current month sync failed for %s: %s", title, username, exc)
                errors += 1
                continue

            player = players[result["username"]]
            player["last_game_sync_at"] = result["checked_at"]

            if result["status"] == "updated":
                player["current_month_key"] = month_key
                player["current_month_etag"] = result["etag"]
                player["current_month_last_modified"] = result["last_modified"]
                player["current_month_game_key"] = result["object_key"]
                player["current_month_game_count"] = result["game_count"]
                updated += 1
            elif result["status"] == "not_modified":
                not_modified += 1
            else:
                empty += 1

    state_store.save_title_state(title, state)
    logger.info(
        "[%s] current month sync done | updated=%d | unchanged=%d | empty=%d | errors=%d",
        title,
        updated,
        not_modified,
        empty,
        errors,
    )


def _backfill_player_months(username: str, current_month: str) -> dict:
    player_index = state_store.load_player_index(username)
    pending = [
        month_key
        for month_key in player_index.get("pending_backfill_months", [])
        if month_key != current_month
    ]
    pending = pending[: config.BACKFILL_MONTHS_PER_PLAYER]

    if not pending:
        return {
            "username": username,
            "processed": 0,
            "remaining": len(player_index.get("pending_backfill_months", [])),
        }

    stored_months = set(player_index.get("stored_months", []))
    processed = 0

    for month_key in pending:
        year, month = config.split_month_key(month_key)
        object_key = config.player_games_key(username, year, month)

        if not storage_client.object_exists(object_key):
            response = chess_client.get_games_for_month_if_changed(username, year, month)
            if response.status_code == 404 or not response.data:
                continue

            storage_client.upload_json(
                object_key,
                {
                    "username": username,
                    "year": year,
                    "month": month,
                    "month_key": month_key,
                    "game_count": len(response.data.get("games", [])),
                    "games": response.data.get("games", []),
                },
                metadata={
                    "source-etag": response.etag,
                    "source-last-modified": response.last_modified,
                },
            )

        stored_months.add(month_key)
        processed += 1

    remaining_pending = [
        month_key
        for month_key in player_index.get("pending_backfill_months", [])
        if month_key not in stored_months
    ]
    player_index["stored_months"] = sorted(stored_months)
    player_index["pending_backfill_months"] = remaining_pending
    state_store.save_player_index(username, player_index)

    return {
        "username": username,
        "processed": processed,
        "remaining": len(remaining_pending),
    }


def run_player_games_backfill(title: str, ds: str) -> None:
    state = state_store.load_title_state(title)
    players = state.get("players", {})
    if not players:
        logger.warning("[%s] no players found in title state", title)
        return

    current_month = config.current_month_key(ds)
    queued_players = [
        username
        for username, player in players.items()
        if player.get("backfill_pending_count", 0) > 0
    ][: config.BACKFILL_PLAYERS_PER_RUN]

    if not queued_players:
        logger.info("[%s] backfill queue is empty", title)
        return

    processed_months = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=config.TITLE_TASK_MAX_WORKERS) as executor:
        futures = {
            executor.submit(_backfill_player_months, username, current_month): username
            for username in queued_players
        }

        for future in as_completed(futures):
            username = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                logger.error("[%s] backfill failed for %s: %s", title, username, exc)
                errors += 1
                continue

            username = result["username"]
            players[username]["backfill_pending_count"] = result["remaining"]
            players[username]["backfill_pending"] = result["remaining"] > 0
            processed_months += result["processed"]

    state_store.save_title_state(title, state)
    logger.info(
        "[%s] backfill run done | players=%d | months_processed=%d | errors=%d",
        title,
        len(queued_players),
        processed_months,
        errors,
    )
