"""
Bronze-layer parquet materializations built from the raw JSON landing zone.
"""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq

import chess_client
import config
import month_selection
import state_store
import storage_client

logger = logging.getLogger(__name__)

DRAW_RESULTS = {
    "agreed",
    "repetition",
    "stalemate",
    "insufficient",
    "50move",
    "timevsinsufficient",
}


def _bucket_for_value(value: str) -> str:
    digest = hashlib.md5(value.encode("utf-8")).hexdigest()
    bucket_id = int(digest[:8], 16) % max(config.PARQUET_BUCKET_COUNT, 1)
    return f"{bucket_id:02d}"


def _iso_from_epoch(value) -> str | None:
    if value is None:
        return None

    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return None


def _date_from_iso(value: str | None) -> str | None:
    if not value:
        return None
    return value[:10]


def _coerce_float(value) -> float | None:
    if value is None or value == "":
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _result_score(result: str | None) -> float | None:
    if not result:
        return None
    if result == "win":
        return 1.0
    if result in DRAW_RESULTS:
        return 0.5
    return 0.0


def _winner_color(white_score: float | None, black_score: float | None) -> str | None:
    if white_score is None or black_score is None:
        return None
    if white_score == black_score:
        return "draw"
    return "white" if white_score > black_score else "black"


def _opening_name(eco_url: str | None) -> str | None:
    if not eco_url:
        return None

    path = urlparse(eco_url).path.rstrip("/")
    if not path:
        return None
    return path.split("/")[-1].replace("-", " ")


def _parquet_buffer(rows: list[dict]) -> BytesIO:
    table = pa.Table.from_pylist(rows)
    buffer = BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)
    return buffer


def _write_bucketed_rows(
    rows: list[dict],
    prefix: str,
    key_builder,
    bucket_value_builder,
    dataset_name: str,
    metadata: dict | None = None,
) -> None:
    storage_client.delete_prefix(prefix)
    if not rows:
        logger.info("%s dataset has no rows for %s", dataset_name, prefix)
        return

    buckets: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        buckets[bucket_value_builder(row)].append(row)

    for bucket, bucket_rows in buckets.items():
        storage_client.upload_parquet_bytes(
            key_builder(bucket),
            _parquet_buffer(bucket_rows),
            metadata={
                **(metadata or {}),
                "dataset": dataset_name,
                "row_count": str(len(bucket_rows)),
            },
        )


def _build_title_lookup() -> dict[str, str]:
    lookup: dict[str, str] = {}
    for title in chess_client.VALID_TITLES:
        state = state_store.load_title_state(title)
        for username in state.get("players", {}):
            lookup.setdefault(username, title)
    return lookup


def _merge_game_payload(existing: dict, incoming: dict) -> None:
    for field in ("accuracies", "pgn", "tcn", "eco", "fen", "initial_setup"):
        if not existing.get(field) and incoming.get(field):
            existing[field] = incoming[field]


def _month_state_game_keys(month_key: str) -> list[str]:
    year, month = config.split_month_key(month_key)
    raw_keys: set[str] = set()
    seen_usernames: set[str] = set()

    for title in chess_client.VALID_TITLES:
        state = state_store.load_title_state(title)
        for username, player in state.get("players", {}).items():
            if username in seen_usernames:
                continue
            seen_usernames.add(username)

            current_month_key = player.get("current_month_key")
            current_month_game_key = player.get("current_month_game_key")
            if current_month_key == month_key and current_month_game_key:
                raw_keys.add(current_month_game_key)
                continue

            player_index = state_store.load_player_index(username)
            if month_key in set(player_index.get("stored_months", [])):
                raw_keys.add(config.player_games_key(username, year, month))

    return sorted(raw_keys)


def _current_month_game_keys(month_key: str) -> list[str]:
    raw_keys: set[str] = set()
    for title in chess_client.VALID_TITLES:
        state = state_store.load_title_state(title)
        for player in state.get("players", {}).values():
            if player.get("current_month_key") == month_key and player.get("current_month_game_key"):
                raw_keys.add(player["current_month_game_key"])
    return sorted(raw_keys)


def _normalize_unique_games(raw_keys: list[str], title_lookup: dict[str, str]) -> tuple[list[dict], list[dict]]:
    games_by_uuid: dict[str, dict] = {}

    for raw_key in raw_keys:
        payload = storage_client.download_json(raw_key) or {}
        source_username = payload.get("username")
        games = payload.get("games", [])

        for game in games:
            game_uuid = game.get("uuid") or game.get("url")
            if not game_uuid:
                continue

            existing = games_by_uuid.get(game_uuid)
            if existing is None:
                games_by_uuid[game_uuid] = {
                    "game": dict(game),
                    "source_keys": {raw_key},
                    "source_usernames": {source_username} if source_username else set(),
                }
                continue

            existing["source_keys"].add(raw_key)
            if source_username:
                existing["source_usernames"].add(source_username)
            _merge_game_payload(existing["game"], game)

    games_core_rows: list[dict] = []
    player_game_rows: list[dict] = []

    for game_uuid, payload in games_by_uuid.items():
        game = payload["game"]
        white = game.get("white") or {}
        black = game.get("black") or {}

        white_username = white.get("username")
        black_username = black.get("username")
        white_title = title_lookup.get(white_username)
        black_title = title_lookup.get(black_username)
        white_result = white.get("result")
        black_result = black.get("result")
        white_score = _result_score(white_result)
        black_score = _result_score(black_result)
        end_time_utc = _iso_from_epoch(game.get("end_time"))
        game_date = _date_from_iso(end_time_utc)
        opening_url = game.get("eco")
        accuracies = game.get("accuracies") or {}
        white_accuracy = _coerce_float(accuracies.get("white")) if isinstance(accuracies, dict) else None
        black_accuracy = _coerce_float(accuracies.get("black")) if isinstance(accuracies, dict) else None

        games_core_rows.append(
            {
                "game_uuid": game_uuid,
                "game_url": game.get("url"),
                "game_date": game_date,
                "end_time_utc": end_time_utc,
                "time_class": game.get("time_class"),
                "time_control": game.get("time_control"),
                "rated": game.get("rated"),
                "rules": game.get("rules"),
                "opening_url": opening_url,
                "opening_name": _opening_name(opening_url),
                "initial_setup": game.get("initial_setup"),
                "fen": game.get("fen"),
                "has_pgn": bool(game.get("pgn")),
                "has_tcn": bool(game.get("tcn")),
                "has_accuracies": isinstance(accuracies, dict) and bool(accuracies),
                "white_username": white_username,
                "white_title": white_title,
                "white_rating": _coerce_float(white.get("rating")),
                "white_result": white_result,
                "white_score": white_score,
                "white_accuracy": white_accuracy,
                "black_username": black_username,
                "black_title": black_title,
                "black_rating": _coerce_float(black.get("rating")),
                "black_result": black_result,
                "black_score": black_score,
                "black_accuracy": black_accuracy,
                "winner_color": _winner_color(white_score, black_score),
                "source_key_count": len(payload["source_keys"]),
                "source_player_count": len(payload["source_usernames"]),
                "source_player_usernames": sorted(payload["source_usernames"]),
            }
        )

        white_rating = _coerce_float(white.get("rating"))
        black_rating = _coerce_float(black.get("rating"))
        shared_fields = {
            "game_uuid": game_uuid,
            "game_url": game.get("url"),
            "game_date": game_date,
            "end_time_utc": end_time_utc,
            "time_class": game.get("time_class"),
            "time_control": game.get("time_control"),
            "rated": game.get("rated"),
            "rules": game.get("rules"),
            "opening_url": opening_url,
            "opening_name": _opening_name(opening_url),
            "winner_color": _winner_color(white_score, black_score),
            "has_pgn": bool(game.get("pgn")),
            "has_tcn": bool(game.get("tcn")),
            "has_accuracies": isinstance(accuracies, dict) and bool(accuracies),
        }

        if white_username:
            player_game_rows.append(
                {
                    **shared_fields,
                    "username": white_username,
                    "title": white_title,
                    "opponent_username": black_username,
                    "opponent_title": black_title,
                    "color": "white",
                    "rating": white_rating,
                    "opponent_rating": black_rating,
                    "rating_diff": white_rating - black_rating if white_rating is not None and black_rating is not None else None,
                    "result": white_result,
                    "score": white_score,
                    "accuracy": white_accuracy,
                    "opponent_accuracy": black_accuracy,
                    "is_titled_player": white_title is not None,
                    "is_titled_opponent": black_title is not None,
                }
            )

        if black_username:
            player_game_rows.append(
                {
                    **shared_fields,
                    "username": black_username,
                    "title": black_title,
                    "opponent_username": white_username,
                    "opponent_title": white_title,
                    "color": "black",
                    "rating": black_rating,
                    "opponent_rating": white_rating,
                    "rating_diff": black_rating - white_rating if white_rating is not None and black_rating is not None else None,
                    "result": black_result,
                    "score": black_score,
                    "accuracy": black_accuracy,
                    "opponent_accuracy": white_accuracy,
                    "is_titled_player": black_title is not None,
                    "is_titled_opponent": white_title is not None,
                }
            )

    return games_core_rows, player_game_rows


def materialize_title_roster_snapshot(title: str, ds: str) -> None:
    snapshot = storage_client.download_json(config.titled_players_snapshot_key(title, ds))
    if not snapshot:
        logger.warning("[%s] no raw titled_players snapshot found for %s", title, ds)
        return

    players = snapshot.get("players", [])
    rows = [
        {
            "username": username,
            "snapshot_player_count": len(players),
            "snapshot_position": position + 1,
        }
        for position, username in enumerate(players)
    ]
    if not rows:
        logger.warning("[%s] raw titled_players snapshot has no players for %s", title, ds)
        return

    storage_client.upload_parquet_bytes(
        config.bronze_roster_daily_key(title, ds),
        _parquet_buffer(rows),
        metadata={
            "dataset": "roster_daily",
            "title": title,
            "snapshot_date": ds,
            "row_count": str(len(rows)),
        },
    )
    logger.info("[%s] bronze roster snapshot materialized | rows=%d", title, len(rows))


def _materialize_games_month(month_key: str, raw_keys: list[str], mode: str) -> None:
    if not raw_keys:
        logger.warning("bronze %s materialization skipped | month=%s | no raw keys", mode, month_key)
        return

    title_lookup = _build_title_lookup()
    games_core_rows, player_game_rows = _normalize_unique_games(raw_keys, title_lookup)
    if not games_core_rows:
        logger.warning("bronze %s materialization found no games | month=%s", mode, month_key)
        return

    _write_bucketed_rows(
        rows=games_core_rows,
        prefix=config.bronze_games_core_prefix(month_key),
        key_builder=lambda bucket: config.bronze_games_core_key(month_key, bucket),
        bucket_value_builder=lambda row: _bucket_for_value(row["game_uuid"]),
        dataset_name="games_core",
        metadata={"month_key": month_key, "mode": mode},
    )
    _write_bucketed_rows(
        rows=player_game_rows,
        prefix=config.bronze_player_game_facts_prefix(month_key),
        key_builder=lambda bucket: config.bronze_player_game_facts_key(month_key, bucket),
        bucket_value_builder=lambda row: _bucket_for_value(f"{row['game_uuid']}::{row['username']}"),
        dataset_name="player_game_facts",
        metadata={"month_key": month_key, "mode": mode},
    )

    logger.info(
        "bronze %s materialization complete | month=%s | raw_files=%d | unique_games=%d | player_rows=%d",
        mode,
        month_key,
        len(raw_keys),
        len(games_core_rows),
        len(player_game_rows),
    )


def materialize_current_month(ds: str) -> None:
    month_key = config.current_month_key(ds)
    _materialize_games_month(month_key, _current_month_game_keys(month_key), "current_month")


def materialize_month_from_state(month_key: str) -> None:
    validated_month_key = month_selection.validate_month_key(month_key)
    _materialize_games_month(validated_month_key, _month_state_game_keys(validated_month_key), "state_scan")


def resolve_month_keys_from_conf(conf: dict | None, ds: str) -> list[str]:
    return month_selection.resolve_month_keys_from_conf(conf, ds)


def materialize_selected_months(month_keys: list[str]) -> None:
    for month_key in month_keys:
        materialize_month_from_state(month_key)
