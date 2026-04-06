import os
from datetime import datetime, timezone


RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw")
STATE_PREFIX = os.environ.get("STATE_PREFIX", "state")
WAREHOUSE_PREFIX = os.environ.get("WAREHOUSE_PREFIX", "warehouse")
BRONZE_PREFIX = f"{WAREHOUSE_PREFIX}/bronze"
SILVER_PREFIX = f"{WAREHOUSE_PREFIX}/silver"
GOLD_PREFIX = f"{WAREHOUSE_PREFIX}/gold"

ARCHIVE_REFRESH_DAYS = int(os.environ.get("ARCHIVE_REFRESH_DAYS", "30"))
ACTIVE_ARCHIVE_REFRESH_HOURS = int(os.environ.get("ACTIVE_ARCHIVE_REFRESH_HOURS", "24"))
TITLE_TASK_MAX_WORKERS = int(os.environ.get("TITLE_TASK_MAX_WORKERS", "3"))
STATE_SCAN_MAX_WORKERS = int(os.environ.get("STATE_SCAN_MAX_WORKERS", "32"))
BACKFILL_PLAYERS_PER_RUN = int(os.environ.get("BACKFILL_PLAYERS_PER_RUN", "10"))
BACKFILL_MONTHS_PER_PLAYER = int(os.environ.get("BACKFILL_MONTHS_PER_PLAYER", "6"))
PARQUET_BUCKET_COUNT = int(os.environ.get("PARQUET_BUCKET_COUNT", os.environ.get("SILVER_BUCKET_COUNT", "16")))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def compact_timestamp(value: str | None = None) -> str:
    value = value or utc_now_iso()
    return value.replace(":", "-")


def current_month_key(ds: str | None = None) -> str:
    if ds:
        return ds[:7]
    return utc_now().strftime("%Y-%m")


def split_month_key(month_key: str) -> tuple[str, str]:
    year, month = month_key.split("-")
    return year, month


def username_prefix(username: str) -> str:
    prefix = username[:2].lower()
    return prefix if prefix else "__"


def title_state_key(title: str) -> str:
    return f"{STATE_PREFIX}/title_state/title={title}/current.json"


def player_index_key(username: str) -> str:
    prefix = username_prefix(username)
    return f"{STATE_PREFIX}/player_index/username_prefix={prefix}/username={username}/index.json"


def titled_players_snapshot_key(title: str, ds: str) -> str:
    return f"{RAW_PREFIX}/titled_players/{title}/{ds}.json"


def player_archives_snapshot_key(username: str, snapshot_date: str) -> str:
    return f"{RAW_PREFIX}/player_archives/{username}/{snapshot_date}.json"


def player_games_key(username: str, year: str, month: str) -> str:
    month = str(month).zfill(2)
    return f"{RAW_PREFIX}/player_games/year={year}/month={month}/username={username}.json"


def titled_players_prefix(title: str) -> str:
    return f"{RAW_PREFIX}/titled_players/{title}/"


def player_archives_prefix(username: str) -> str:
    return f"{RAW_PREFIX}/player_archives/{username}/"


def player_games_month_prefix(year: str, month: str) -> str:
    month = str(month).zfill(2)
    return f"{RAW_PREFIX}/player_games/year={year}/month={month}/"


def legacy_player_games_prefix(username: str) -> str:
    # Legacy raw layout: raw/player_games/{username}/{year}/{month}.json.
    # Retained only for one-time bootstrap from pre-migration data.
    return f"{RAW_PREFIX}/player_games/{username}/"


def bronze_roster_daily_key(title: str, ds: str) -> str:
    return f"{BRONZE_PREFIX}/roster_daily/snapshot_date={ds}/title={title}/part-000.parquet"


def bronze_games_core_prefix(month_key: str) -> str:
    year, month = split_month_key(month_key)
    return f"{BRONZE_PREFIX}/games_core/year={year}/month={month}/"


def bronze_games_core_key(month_key: str, bucket: str) -> str:
    return f"{bronze_games_core_prefix(month_key)}bucket={bucket}/part-000.parquet"


def bronze_player_game_facts_prefix(month_key: str) -> str:
    year, month = split_month_key(month_key)
    return f"{BRONZE_PREFIX}/player_game_facts/year={year}/month={month}/"


def bronze_player_game_facts_key(month_key: str, bucket: str) -> str:
    return f"{bronze_player_game_facts_prefix(month_key)}bucket={bucket}/part-000.parquet"


