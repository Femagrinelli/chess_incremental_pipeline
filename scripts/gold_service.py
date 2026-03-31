"""
Gold-layer parquet marts built from the silver player-month dataset.
"""

from __future__ import annotations

import logging
import re
from io import BytesIO

import pyarrow.parquet as pq

import config
import duckdb_utils
import silver_service
import storage_client

logger = logging.getLogger(__name__)

MONTH_KEY_PATTERN = re.compile(r"^\d{4}-\d{2}$")


def _validate_month_key(month_key: str) -> str:
    if not isinstance(month_key, str) or not MONTH_KEY_PATTERN.match(month_key):
        raise ValueError(f"Invalid month_key '{month_key}'. Expected YYYY-MM.")

    year, month = month_key.split("-")
    if not 1 <= int(month) <= 12:
        raise ValueError(f"Invalid month_key '{month_key}'. Month must be between 01 and 12.")
    return month_key


def _player_month_glob(month_key: str) -> str:
    year, month = config.split_month_key(month_key)
    return (
        f"s3://{{{{S3_BUCKET}}}}/{{{{SILVER_PREFIX}}}}/"
        f"player_month/year={year}/month={month}/title=*/bucket=*/part-000.parquet"
    )


def _execute_arrow(sql: str):
    conn = duckdb_utils.create_connection()
    try:
        return conn.execute(duckdb_utils.render_sql(sql)).fetch_arrow_table()
    finally:
        conn.close()


def _upload_table(table, key: str, prefix: str, dataset_name: str, month_key: str) -> None:
    storage_client.delete_prefix(prefix)
    if table.num_rows == 0:
        logger.warning("%s gold dataset has no rows for month=%s", dataset_name, month_key)
        return

    buffer = BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    storage_client.upload_parquet_bytes(
        key,
        buffer,
        metadata={
            "dataset": dataset_name,
            "month_key": month_key,
            "row_count": str(table.num_rows),
        },
    )
    logger.info("%s gold dataset materialized | month=%s | rows=%d", dataset_name, month_key, table.num_rows)


def _activity_sql(month_key: str) -> str:
    return f"""
    SELECT
        month_key,
        year,
        month,
        title,
        COUNT(*) AS active_players,
        SUM(games_played) AS total_games,
        SUM(wins) AS total_wins,
        SUM(draws) AS total_draws,
        SUM(losses) AS total_losses,
        SUM(rated_games) AS rated_games,
        SUM(unrated_games) AS unrated_games,
        SUM(games_vs_titled) AS games_vs_titled,
        SUM(games_vs_non_titled) AS games_vs_non_titled,
        ROUND(SUM(games_played) * 1.0 / NULLIF(COUNT(*), 0), 2) AS avg_games_per_active_player,
        ROUND(AVG(unique_opponent_count), 2) AS avg_unique_opponents,
        ROUND(AVG(win_rate), 4) AS avg_player_win_rate,
        ROUND(AVG(draw_rate), 4) AS avg_player_draw_rate,
        ROUND(AVG(loss_rate), 4) AS avg_player_loss_rate,
        ROUND(AVG(avg_rating), 2) AS avg_player_rating
    FROM read_parquet('{_player_month_glob(month_key)}')
    GROUP BY 1, 2, 3, 4
    ORDER BY title
    """


def _rating_volatility_sql(month_key: str) -> str:
    return f"""
    SELECT
        month_key,
        year,
        month,
        title,
        COUNT(*) AS player_months,
        ROUND(AVG(games_played), 2) AS avg_games_per_player,
        ROUND(AVG(rating_range), 2) AS avg_rating_range,
        ROUND(MEDIAN(rating_range), 2) AS median_rating_range,
        ROUND(MIN(rating_range), 2) AS min_rating_range,
        ROUND(MAX(rating_range), 2) AS max_rating_range,
        ROUND(AVG(rating_stddev), 2) AS avg_rating_stddev,
        ROUND(AVG(avg_rating), 2) AS avg_player_rating
    FROM read_parquet('{_player_month_glob(month_key)}')
    WHERE games_played >= 2
      AND rating_range IS NOT NULL
    GROUP BY 1, 2, 3, 4
    ORDER BY title
    """


def _color_performance_sql(month_key: str) -> str:
    return f"""
    SELECT
        month_key,
        year,
        month,
        title,
        SUM(white_games) AS white_games,
        ROUND(SUM(white_score), 2) AS white_score_points,
        ROUND(SUM(white_score) * 1.0 / NULLIF(SUM(white_games), 0), 4) AS white_score_rate,
        SUM(black_games) AS black_games,
        ROUND(SUM(black_score), 2) AS black_score_points,
        ROUND(SUM(black_score) * 1.0 / NULLIF(SUM(black_games), 0), 4) AS black_score_rate,
        ROUND(
            (SUM(white_score) * 1.0 / NULLIF(SUM(white_games), 0))
            - (SUM(black_score) * 1.0 / NULLIF(SUM(black_games), 0)),
            4
        ) AS white_minus_black,
        SUM(games_played) AS total_games
    FROM read_parquet('{_player_month_glob(month_key)}')
    GROUP BY 1, 2, 3, 4
    ORDER BY title
    """


def materialize_month(month_key: str) -> None:
    validated_month_key = _validate_month_key(month_key)

    activity_table = _execute_arrow(_activity_sql(validated_month_key))
    _upload_table(
        table=activity_table,
        key=config.gold_title_month_activity_key(validated_month_key),
        prefix=config.gold_title_month_activity_prefix(validated_month_key),
        dataset_name="title_month_activity",
        month_key=validated_month_key,
    )

    rating_volatility_table = _execute_arrow(_rating_volatility_sql(validated_month_key))
    _upload_table(
        table=rating_volatility_table,
        key=config.gold_title_month_rating_volatility_key(validated_month_key),
        prefix=config.gold_title_month_rating_volatility_prefix(validated_month_key),
        dataset_name="title_month_rating_volatility",
        month_key=validated_month_key,
    )

    color_performance_table = _execute_arrow(_color_performance_sql(validated_month_key))
    _upload_table(
        table=color_performance_table,
        key=config.gold_title_month_color_performance_key(validated_month_key),
        prefix=config.gold_title_month_color_performance_prefix(validated_month_key),
        dataset_name="title_month_color_performance",
        month_key=validated_month_key,
    )


def materialize_current_month(ds: str) -> None:
    materialize_month(config.current_month_key(ds))


def resolve_month_keys_from_conf(conf: dict | None, ds: str) -> list[str]:
    return silver_service.resolve_month_keys_from_conf(conf, ds)


def materialize_selected_months(month_keys: list[str]) -> None:
    for month_key in month_keys:
        materialize_month(month_key)
