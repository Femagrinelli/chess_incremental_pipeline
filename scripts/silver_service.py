"""
Silver-layer parquet materializations built from the raw JSON landing zone.
"""

from __future__ import annotations

import hashlib
import logging
import re
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq

import chess_client
import config
import state_store
import storage_client

logger = logging.getLogger(__name__)

MONTH_KEY_PATTERN = re.compile(r"^\d{4}-\d{2}$")
YEAR_PATTERN = re.compile(r"^\d{4}$")

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
    bucket_id = int(digest[:8], 16) % max(config.SILVER_BUCKET_COUNT, 1)
    return f"{bucket_id:02d}"


def _validate_month_key(month_key: str) -> str:
    if not isinstance(month_key, str) or not MONTH_KEY_PATTERN.match(month_key):
        raise ValueError(f"Invalid month_key '{month_key}'. Expected YYYY-MM.")

    year, month = month_key.split("-")
    if not 1 <= int(month) <= 12:
        raise ValueError(f"Invalid month_key '{month_key}'. Month must be between 01 and 12.")
    return month_key


def _validate_year(year: str) -> str:
    year_str = str(year)
    if not YEAR_PATTERN.match(year_str):
        raise ValueError(f"Invalid year '{year}'. Expected YYYY.")
    return year_str


def _parse_string_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    raise ValueError(f"Expected string or list, got {type(value).__name__}.")


def _month_range(start_month: str, end_month: str) -> list[str]:
    start_value = _validate_month_key(start_month)
    end_value = _validate_month_key(end_month)

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


def _available_month_keys_from_state() -> list[str]:
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
                month_keys.add(_validate_month_key(current_month_key))

            player_index = state_store.load_player_index(username)
            for month_key in player_index.get("stored_months", []):
                month_keys.add(_validate_month_key(month_key))

    return sorted(month_keys)


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


def _title_month_game_keys(title: str, month_key: str, include_stored_months: bool) -> list[str]:
    raw_keys: set[str] = set()
    year, month = config.split_month_key(month_key)
    state = state_store.load_title_state(title)

    for username, player in state.get("players", {}).items():
        current_month_key = player.get("current_month_key")
        current_month_game_key = player.get("current_month_game_key")
        if current_month_key == month_key and current_month_game_key:
            raw_keys.add(current_month_game_key)
            continue

        if not include_stored_months:
            continue

        player_index = state_store.load_player_index(username)
        if month_key in set(player_index.get("stored_months", [])):
            raw_keys.add(config.player_games_key(username, year, month))

    return sorted(raw_keys)


def _merge_game_payload(existing: dict, incoming: dict) -> None:
    for field in ("accuracies", "pgn", "tcn", "eco", "fen", "initial_setup"):
        if not existing.get(field) and incoming.get(field):
            existing[field] = incoming[field]


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

        core_row = {
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
        games_core_rows.append(core_row)

        white_rating = core_row["white_rating"]
        black_rating = core_row["black_rating"]
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
            "winner_color": core_row["winner_color"],
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


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _stdev(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    return statistics.pstdev(values)


def _build_player_month_rows(player_game_rows: list[dict], month_key: str) -> list[dict]:
    year, month = config.split_month_key(month_key)
    grouped: dict[tuple[str, str | None], dict] = {}

    for row in player_game_rows:
        if row.get("title") is None:
            continue

        key = (row["username"], row.get("title"))
        current = grouped.get(key)
        if current is None:
            current = {
                "month_key": month_key,
                "year": year,
                "month": month,
                "username": row["username"],
                "title": row.get("title"),
                "games_played": 0,
                "wins": 0,
                "draws": 0,
                "losses": 0,
                "score": 0.0,
                "white_games": 0,
                "black_games": 0,
                "white_score": 0.0,
                "black_score": 0.0,
                "rated_games": 0,
                "unique_opponents": set(),
                "ratings": [],
                "opponent_ratings": [],
                "rating_diffs": [],
                "bullet_games": 0,
                "blitz_games": 0,
                "rapid_games": 0,
                "daily_games": 0,
                "games_vs_titled": 0,
            }
            grouped[key] = current

        score = row.get("score")
        current["games_played"] += 1
        current["wins"] += 1 if score == 1.0 else 0
        current["draws"] += 1 if score == 0.5 else 0
        current["losses"] += 1 if score == 0.0 else 0
        current["score"] += score or 0.0
        current["white_games"] += 1 if row.get("color") == "white" else 0
        current["black_games"] += 1 if row.get("color") == "black" else 0
        current["white_score"] += (score or 0.0) if row.get("color") == "white" else 0.0
        current["black_score"] += (score or 0.0) if row.get("color") == "black" else 0.0
        current["rated_games"] += 1 if row.get("rated") else 0
        current["games_vs_titled"] += 1 if row.get("is_titled_opponent") else 0

        opponent_username = row.get("opponent_username")
        if opponent_username:
            current["unique_opponents"].add(opponent_username)

        rating = row.get("rating")
        if rating is not None:
            current["ratings"].append(rating)

        opponent_rating = row.get("opponent_rating")
        if opponent_rating is not None:
            current["opponent_ratings"].append(opponent_rating)

        rating_diff = row.get("rating_diff")
        if rating_diff is not None:
            current["rating_diffs"].append(rating_diff)

        time_class = row.get("time_class")
        if time_class == "bullet":
            current["bullet_games"] += 1
        elif time_class == "blitz":
            current["blitz_games"] += 1
        elif time_class == "rapid":
            current["rapid_games"] += 1
        elif time_class == "daily":
            current["daily_games"] += 1

    rows: list[dict] = []
    for current in grouped.values():
        games_played = current["games_played"]
        ratings = current.pop("ratings")
        opponent_ratings = current.pop("opponent_ratings")
        rating_diffs = current.pop("rating_diffs")
        unique_opponents = current.pop("unique_opponents")

        row = {
            **current,
            "games_played": games_played,
            "win_rate": current["wins"] / games_played if games_played else None,
            "draw_rate": current["draws"] / games_played if games_played else None,
            "loss_rate": current["losses"] / games_played if games_played else None,
            "white_score_rate": current["white_score"] / current["white_games"] if current["white_games"] else None,
            "black_score_rate": current["black_score"] / current["black_games"] if current["black_games"] else None,
            "unrated_games": games_played - current["rated_games"],
            "games_vs_non_titled": games_played - current["games_vs_titled"],
            "unique_opponent_count": len(unique_opponents),
            "avg_rating": _mean(ratings),
            "min_rating": min(ratings) if ratings else None,
            "max_rating": max(ratings) if ratings else None,
            "rating_range": (max(ratings) - min(ratings)) if ratings else None,
            "rating_stddev": _stdev(ratings),
            "avg_opponent_rating": _mean(opponent_ratings),
            "avg_rating_diff": _mean(rating_diffs),
        }
        rows.append(row)

    return rows


def _normalize_title_player_games(
    raw_keys: list[str],
    default_title: str,
    title_lookup: dict[str, str],
    mode: str,
) -> list[dict]:
    player_game_rows: list[dict] = []

    for index, raw_key in enumerate(raw_keys, start=1):
        payload = storage_client.download_json(raw_key) or {}
        source_username = payload.get("username")
        if not source_username:
            logger.warning("[%s] skipped raw file without username | key=%s", default_title, raw_key)
            continue

        source_title = title_lookup.get(source_username) or default_title
        seen_game_ids: set[str] = set()
        games = payload.get("games", [])

        for game in games:
            game_uuid = game.get("uuid") or game.get("url")
            if not game_uuid or game_uuid in seen_game_ids:
                continue
            seen_game_ids.add(game_uuid)

            white = game.get("white") or {}
            black = game.get("black") or {}
            white_username = white.get("username")
            black_username = black.get("username")

            if white_username == source_username:
                color = "white"
                source_player = white
                opponent_player = black
                opponent_username = black_username
            elif black_username == source_username:
                color = "black"
                source_player = black
                opponent_player = white
                opponent_username = white_username
            else:
                logger.warning(
                    "[%s] skipped game with mismatched source player | key=%s | source=%s | game_uuid=%s",
                    default_title,
                    raw_key,
                    source_username,
                    game_uuid,
                )
                continue

            result = source_player.get("result")
            score = _result_score(result)
            opponent_title = title_lookup.get(opponent_username)
            accuracies = game.get("accuracies") or {}
            source_accuracy = None
            opponent_accuracy = None
            if isinstance(accuracies, dict):
                if color == "white":
                    source_accuracy = _coerce_float(accuracies.get("white"))
                    opponent_accuracy = _coerce_float(accuracies.get("black"))
                else:
                    source_accuracy = _coerce_float(accuracies.get("black"))
                    opponent_accuracy = _coerce_float(accuracies.get("white"))

            rating = _coerce_float(source_player.get("rating"))
            opponent_rating = _coerce_float(opponent_player.get("rating"))
            opening_url = game.get("eco")
            end_time_utc = _iso_from_epoch(game.get("end_time"))

            player_game_rows.append(
                {
                    "game_uuid": game_uuid,
                    "game_url": game.get("url"),
                    "game_date": _date_from_iso(end_time_utc),
                    "end_time_utc": end_time_utc,
                    "time_class": game.get("time_class"),
                    "time_control": game.get("time_control"),
                    "rated": game.get("rated"),
                    "rules": game.get("rules"),
                    "opening_url": opening_url,
                    "opening_name": _opening_name(opening_url),
                    "winner_color": _winner_color(
                        _result_score(white.get("result")),
                        _result_score(black.get("result")),
                    ),
                    "username": source_username,
                    "title": source_title,
                    "opponent_username": opponent_username,
                    "opponent_title": opponent_title,
                    "color": color,
                    "rating": rating,
                    "opponent_rating": opponent_rating,
                    "rating_diff": rating - opponent_rating if rating is not None and opponent_rating is not None else None,
                    "result": result,
                    "score": score,
                    "accuracy": source_accuracy,
                    "opponent_accuracy": opponent_accuracy,
                    "is_titled_player": True,
                    "is_titled_opponent": opponent_title is not None,
                    "source_key": raw_key,
                    "source_title": default_title,
                    "has_pgn": bool(game.get("pgn")),
                    "has_tcn": bool(game.get("tcn")),
                    "has_accuracies": isinstance(accuracies, dict) and bool(accuracies),
                }
            )

        if index == 1 or index % 250 == 0 or index == len(raw_keys):
            logger.info(
                "[%s] silver %s progress | raw_files=%d/%d | player_rows=%d",
                default_title,
                mode,
                index,
                len(raw_keys),
                len(player_game_rows),
            )

    return player_game_rows


def materialize_title_roster_snapshot(title: str, ds: str) -> None:
    snapshot = storage_client.download_json(config.titled_players_snapshot_key(title, ds))
    if not snapshot:
        logger.warning("[%s] no raw titled_players snapshot found for %s", title, ds)
        return

    players = snapshot.get("players", [])
    rows = [
        {
            "snapshot_date": ds,
            "title": title,
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
        config.silver_title_roster_key(title, ds),
        _parquet_buffer(rows),
        metadata={
            "dataset": "title_roster_daily",
            "title": title,
            "snapshot_date": ds,
            "row_count": str(len(rows)),
        },
    )
    logger.info("[%s] silver title roster snapshot materialized | rows=%d", title, len(rows))


def _materialize_title_player_month(
    title: str,
    month_key: str,
    raw_keys: list[str],
    mode: str,
    title_lookup: dict[str, str] | None = None,
) -> None:
    if not raw_keys:
        logger.warning("[%s] silver %s skipped | month=%s | no raw keys", title, mode, month_key)
        return

    title_lookup = title_lookup or _build_title_lookup()
    player_game_rows = _normalize_title_player_games(raw_keys, title, title_lookup, mode)
    if not player_game_rows:
        logger.warning("[%s] silver %s found no player rows | month=%s", title, mode, month_key)
        return

    year, month = config.split_month_key(month_key)
    for row in player_game_rows:
        row["month_key"] = month_key
        row["year"] = year
        row["month"] = month

    player_month_rows = _build_player_month_rows(player_game_rows, month_key)

    _write_bucketed_rows(
        rows=player_game_rows,
        prefix=config.silver_player_games_prefix(month_key, title),
        key_builder=lambda bucket: config.silver_player_games_key(month_key, bucket, title),
        bucket_value_builder=lambda row: _bucket_for_value(f"{row['game_uuid']}::{row['username']}"),
        dataset_name="player_games",
        metadata={"month_key": month_key, "mode": mode, "title": title},
    )
    _write_bucketed_rows(
        rows=player_month_rows,
        prefix=config.silver_player_month_prefix(month_key, title),
        key_builder=lambda bucket: config.silver_player_month_key(month_key, bucket, title),
        bucket_value_builder=lambda row: _bucket_for_value(row["username"]),
        dataset_name="player_month",
        metadata={"month_key": month_key, "mode": mode, "title": title},
    )

    logger.info(
        "[%s] silver %s materialization complete | month=%s | raw_files=%d | player_rows=%d | player_month_rows=%d",
        title,
        mode,
        month_key,
        len(raw_keys),
        len(player_game_rows),
        len(player_month_rows),
    )


def _materialize_games_month(month_key: str, raw_keys: list[str], mode: str) -> None:
    if not raw_keys:
        logger.warning("silver %s materialization skipped | month=%s | no raw keys", mode, month_key)
        return

    title_lookup = _build_title_lookup()
    games_core_rows, player_game_rows = _normalize_unique_games(raw_keys, title_lookup)
    if not games_core_rows:
        logger.warning("silver %s materialization found no games | month=%s", mode, month_key)
        return

    year, month = config.split_month_key(month_key)
    for row in games_core_rows:
        row["month_key"] = month_key
        row["year"] = year
        row["month"] = month

    for row in player_game_rows:
        row["month_key"] = month_key
        row["year"] = year
        row["month"] = month

    player_month_rows = _build_player_month_rows(player_game_rows, month_key)

    _write_bucketed_rows(
        rows=games_core_rows,
        prefix=config.silver_games_core_prefix(month_key),
        key_builder=lambda bucket: config.silver_games_core_key(month_key, bucket),
        bucket_value_builder=lambda row: _bucket_for_value(row["game_uuid"]),
        dataset_name="games_core",
        metadata={"month_key": month_key, "mode": mode},
    )
    _write_bucketed_rows(
        rows=player_game_rows,
        prefix=config.silver_player_games_prefix(month_key),
        key_builder=lambda bucket: config.silver_player_games_key(month_key, bucket),
        bucket_value_builder=lambda row: _bucket_for_value(f"{row['game_uuid']}::{row['username']}"),
        dataset_name="player_games",
        metadata={"month_key": month_key, "mode": mode},
    )
    _write_bucketed_rows(
        rows=player_month_rows,
        prefix=config.silver_player_month_prefix(month_key),
        key_builder=lambda bucket: config.silver_player_month_key(month_key, bucket),
        bucket_value_builder=lambda row: _bucket_for_value(row["username"]),
        dataset_name="player_month",
        metadata={"month_key": month_key, "mode": mode},
    )

    logger.info(
        "silver %s materialization complete | month=%s | raw_files=%d | unique_games=%d | player_rows=%d | player_month_rows=%d",
        mode,
        month_key,
        len(raw_keys),
        len(games_core_rows),
        len(player_game_rows),
        len(player_month_rows),
    )


def materialize_current_month(ds: str) -> None:
    month_key = config.current_month_key(ds)
    title_lookup = _build_title_lookup()
    for title in chess_client.VALID_TITLES:
        materialize_current_month_title(title=title, ds=ds, title_lookup=title_lookup)


def materialize_month_from_state(month_key: str) -> None:
    validated_month_key = _validate_month_key(month_key)
    _materialize_games_month(validated_month_key, _month_state_game_keys(validated_month_key), "state_scan")


def materialize_current_month_title(title: str, ds: str, title_lookup: dict[str, str] | None = None) -> None:
    month_key = config.current_month_key(ds)
    raw_keys = _title_month_game_keys(title=title, month_key=month_key, include_stored_months=False)
    _materialize_title_player_month(
        title=title,
        month_key=month_key,
        raw_keys=raw_keys,
        mode="current_month",
        title_lookup=title_lookup,
    )


def materialize_month_title_from_state(
    title: str,
    month_key: str,
    title_lookup: dict[str, str] | None = None,
) -> None:
    validated_month_key = _validate_month_key(month_key)
    raw_keys = _title_month_game_keys(title=title, month_key=validated_month_key, include_stored_months=True)
    _materialize_title_player_month(
        title=title,
        month_key=validated_month_key,
        raw_keys=raw_keys,
        mode="state_scan",
        title_lookup=title_lookup,
    )


def resolve_month_keys_from_conf(conf: dict | None, ds: str) -> list[str]:
    conf = conf or {}
    month_keys: set[str] = set()

    for month_key in _parse_string_list(conf.get("month_keys")):
        month_keys.add(_validate_month_key(month_key))

    if conf.get("month_key"):
        month_keys.add(_validate_month_key(str(conf["month_key"])))

    years = {
        _validate_year(year)
        for year in _parse_string_list(conf.get("years"))
    }
    if conf.get("year"):
        years.add(_validate_year(conf["year"]))

    start_month = conf.get("start_month")
    end_month = conf.get("end_month")
    if start_month or end_month:
        if not start_month or not end_month:
            raise ValueError("start_month and end_month must be provided together.")
        month_keys.update(_month_range(str(start_month), str(end_month)))

    if years:
        available_months = _available_month_keys_from_state()
        matched_months = [
            month_key
            for month_key in available_months
            if month_key[:4] in years
        ]
        if not matched_months:
            logger.warning("No month_keys found in state for years=%s", sorted(years))
        month_keys.update(matched_months)

    if not month_keys:
        month_keys.add(_validate_month_key(ds[:7]))

    resolved = sorted(month_keys)
    logger.info("Resolved silver month selection | conf=%s | months=%s", conf, resolved)
    return resolved


def materialize_selected_months(month_keys: list[str]) -> None:
    title_lookup = _build_title_lookup()
    for month_key in month_keys:
        for title in chess_client.VALID_TITLES:
            materialize_month_title_from_state(title=title, month_key=month_key, title_lookup=title_lookup)
