"""
DuckDB helper for querying gold parquet datasets in S3.
"""

from __future__ import annotations

import os
from typing import Iterable

import duckdb
import pandas as pd


def _configure_connection(conn: duckdb.DuckDBPyConnection) -> None:
    try:
        conn.execute("LOAD httpfs")
    except duckdb.Error:
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
    conn.execute(f"SET s3_region='{os.environ.get('AWS_REGION', 'us-east-1')}'")
    conn.execute(f"SET s3_access_key_id='{escape_sql_literal(os.environ.get('AWS_ACCESS_KEY_ID', ''))}'")
    conn.execute(f"SET s3_secret_access_key='{escape_sql_literal(os.environ.get('AWS_SECRET_ACCESS_KEY', ''))}'")

    session_token = os.environ.get("AWS_SESSION_TOKEN")
    if session_token:
        conn.execute(f"SET s3_session_token='{escape_sql_literal(session_token)}'")

    endpoint = os.environ.get("S3_ENDPOINT_URL")
    if endpoint:
        conn.execute(f"SET s3_endpoint='{escape_sql_literal(endpoint)}'")


def create_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(database=":memory:")
    _configure_connection(conn)
    return conn


def run_query(sql: str) -> pd.DataFrame:
    conn = create_connection()
    try:
        return conn.execute(sql).fetchdf()
    finally:
        conn.close()


def _warehouse_prefix() -> str:
    return os.environ.get("WAREHOUSE_PREFIX", "warehouse")


def gold_prefix() -> str:
    return f"{_warehouse_prefix()}/gold"


def silver_prefix() -> str:
    return f"{_warehouse_prefix()}/silver"


def s3_bucket() -> str:
    return os.environ.get("S3_BUCKET", "")


def escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def normalize_pair(player_one: str, player_two: str) -> tuple[str, str]:
    left = player_one.strip().lower()
    right = player_two.strip().lower()
    if left <= right:
        return left, right
    return right, left


def sql_in_list(values: Iterable[str]) -> str:
    safe_values = [f"'{escape_sql_literal(value)}'" for value in values]
    if not safe_values:
        return "''"
    return ", ".join(safe_values)
