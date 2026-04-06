"""
One-time migration: rewrite raw/player_games keys from the legacy layout
  raw/player_games/{username}/{year}/{month}.json
to the new layout
  raw/player_games/year={year}/month={month}/username={username}.json

Sharded by an arbitrary username prefix so a large migration can be split
across multiple DAG runs (e.g. prefix="a", "b", ..., or "aa", "ab", ...).
"""

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import config
import storage_client

logger = logging.getLogger(__name__)

LEGACY_KEY_RE = re.compile(
    rf"^{re.escape(config.RAW_PREFIX)}/player_games/"
    r"(?P<username>[^/]+)/(?P<year>\d{4})/(?P<month>\d{2})\.json$"
)

_NEW_LAYOUT_MARKER = f"{config.RAW_PREFIX}/player_games/year="


def _plan_migration(prefix: str) -> tuple[list[tuple[str, str]], int, int]:
    """List all keys under raw/player_games/{prefix} and build (old, new) pairs.

    Returns (pairs, already_new, unrecognized).
    """
    list_prefix = f"{config.RAW_PREFIX}/player_games/{prefix}"
    keys = storage_client.list_objects(list_prefix)

    pairs: list[tuple[str, str]] = []
    already_new = 0
    unrecognized = 0

    for key in keys:
        if key.startswith(_NEW_LAYOUT_MARKER):
            already_new += 1
            continue

        match = LEGACY_KEY_RE.match(key)
        if not match:
            unrecognized += 1
            logger.warning("skipping unrecognized key under player_games: %s", key)
            continue

        new_key = config.player_games_key(
            username=match.group("username"),
            year=match.group("year"),
            month=match.group("month"),
        )
        pairs.append((key, new_key))

    return pairs, already_new, unrecognized


def migrate_prefix(prefix: str, dry_run: bool = False, max_workers: int = 16) -> dict:
    """Migrate every legacy key whose path starts with raw/player_games/{prefix}.

    `prefix` is passed directly as the S3 list prefix, so "ma" shards all
    usernames starting with "ma" (magnuscarlsen, marc, ...). Use "" to migrate
    everything in one run (not recommended for large buckets).
    """
    if not prefix:
        logger.warning("empty prefix — listing the entire player_games tree")

    pairs, already_new, unrecognized = _plan_migration(prefix)

    logger.info(
        "migration plan | prefix=%r | to_move=%d | already_new=%d | unrecognized=%d | dry_run=%s",
        prefix,
        len(pairs),
        already_new,
        unrecognized,
        dry_run,
    )

    if dry_run:
        sample = pairs[:5]
        for src, dst in sample:
            logger.info("[dry_run] %s -> %s", src, dst)
        return {
            "prefix": prefix,
            "planned": len(pairs),
            "moved": 0,
            "already_new": already_new,
            "unrecognized": unrecognized,
            "dry_run": True,
        }

    moved = 0
    failed = 0

    if pairs:
        workers = max(1, min(max_workers, len(pairs)))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(storage_client.move_object, src, dst): (src, dst)
                for src, dst in pairs
            }
            for count, future in enumerate(as_completed(futures), start=1):
                src, dst = futures[future]
                try:
                    future.result()
                    moved += 1
                except Exception as exc:
                    failed += 1
                    logger.error("move failed %s -> %s: %s", src, dst, exc)

                if count % 1000 == 0:
                    logger.info(
                        "migration progress | prefix=%r | %d/%d moved=%d failed=%d",
                        prefix,
                        count,
                        len(pairs),
                        moved,
                        failed,
                    )

    result = {
        "prefix": prefix,
        "planned": len(pairs),
        "moved": moved,
        "failed": failed,
        "already_new": already_new,
        "unrecognized": unrecognized,
        "dry_run": False,
    }
    logger.info("migration done | %s", result)
    return result
