"""
Reusable DuckDB helpers for querying S3-backed silver parquet datasets.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb


def escape_sql(value: str) -> str:
    return value.replace("'", "''")


def render_sql(sql: str) -> str:
    replacements = {
        "{{S3_BUCKET}}": os.environ.get("S3_BUCKET", ""),
        "{{SILVER_PREFIX}}": os.environ.get("SILVER_PREFIX", "silver/chess_com"),
        "{{GOLD_PREFIX}}": os.environ.get("GOLD_PREFIX", "gold/chess_com"),
        "{{AWS_REGION}}": os.environ.get("AWS_REGION", "us-east-1"),
    }
    rendered = sql
    for placeholder, value in replacements.items():
        rendered = rendered.replace(placeholder, value)
    return rendered


def create_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(database=":memory:")
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    conn.execute(f"SET s3_region='{escape_sql(os.environ.get('AWS_REGION', 'us-east-1'))}'")
    conn.execute("SET s3_url_style='path'")

    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    session_token = os.environ.get("AWS_SESSION_TOKEN")
    endpoint_url = (os.environ.get("S3_ENDPOINT_URL") or "").strip()

    if access_key and secret_key:
        conn.execute(f"SET s3_access_key_id='{escape_sql(access_key)}'")
        conn.execute(f"SET s3_secret_access_key='{escape_sql(secret_key)}'")
    if session_token:
        conn.execute(f"SET s3_session_token='{escape_sql(session_token)}'")
    if endpoint_url:
        endpoint = endpoint_url.replace("https://", "").replace("http://", "").rstrip("/")
        conn.execute(f"SET s3_endpoint='{escape_sql(endpoint)}'")

    return conn


def run_sql(sql: str) -> tuple[duckdb.DuckDBPyConnection, str]:
    rendered_sql = render_sql(sql)
    conn = create_connection()
    conn.execute(rendered_sql)
    return conn, rendered_sql


def read_sql_file(path: str | Path) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def list_sql_files(directory: str | Path) -> list[str]:
    base_path = Path(directory)
    if not base_path.exists():
        return []
    return sorted(path.name for path in base_path.glob("*.sql"))
