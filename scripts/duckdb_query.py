#!/usr/bin/env python3
"""
Small helper to run DuckDB queries against S3-backed silver parquet datasets.
"""

from __future__ import annotations

import argparse
import os
import sys

import duckdb


def _escape_sql(value: str) -> str:
    return value.replace("'", "''")


def _render_sql(sql: str) -> str:
    replacements = {
        "{{S3_BUCKET}}": os.environ.get("S3_BUCKET", ""),
        "{{SILVER_PREFIX}}": os.environ.get("SILVER_PREFIX", "silver/chess_com"),
        "{{AWS_REGION}}": os.environ.get("AWS_REGION", "us-east-1"),
    }
    rendered = sql
    for placeholder, value in replacements.items():
        rendered = rendered.replace(placeholder, value)
    return rendered


def _configure_s3(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    conn.execute(f"SET s3_region='{_escape_sql(os.environ.get('AWS_REGION', 'us-east-1'))}'")
    conn.execute("SET s3_url_style='path'")

    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    session_token = os.environ.get("AWS_SESSION_TOKEN")
    endpoint_url = (os.environ.get("S3_ENDPOINT_URL") or "").strip()

    if access_key and secret_key:
        conn.execute(f"SET s3_access_key_id='{_escape_sql(access_key)}'")
        conn.execute(f"SET s3_secret_access_key='{_escape_sql(secret_key)}'")
    if session_token:
        conn.execute(f"SET s3_session_token='{_escape_sql(session_token)}'")
    if endpoint_url:
        endpoint = endpoint_url.replace("https://", "").replace("http://", "").rstrip("/")
        conn.execute(f"SET s3_endpoint='{_escape_sql(endpoint)}'")


def _print_result(conn: duckdb.DuckDBPyConnection) -> None:
    if conn.description is None:
        print("Query executed successfully.")
        return

    columns = [description[0] for description in conn.description]
    rows = conn.fetchall()

    if not rows:
        print(" | ".join(columns))
        print("(0 rows)")
        return

    widths = [len(column) for column in columns]
    for row in rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len("" if value is None else str(value)))

    header = " | ".join(column.ljust(widths[idx]) for idx, column in enumerate(columns))
    divider = "-+-".join("-" * widths[idx] for idx in range(len(columns)))
    print(header)
    print(divider)
    for row in rows:
        print(
            " | ".join(
                ("" if value is None else str(value)).ljust(widths[idx])
                for idx, value in enumerate(row)
            )
        )
    print(f"\nRows: {len(rows)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run DuckDB SQL against silver parquet in S3.")
    parser.add_argument("--sql", help="Inline SQL query to execute.")
    parser.add_argument("--file", help="Path to a SQL file to execute.")
    args = parser.parse_args()

    if bool(args.sql) == bool(args.file):
        parser.error("Provide exactly one of --sql or --file.")

    if args.file:
        with open(args.file, "r", encoding="utf-8") as handle:
            sql = handle.read()
    else:
        sql = args.sql or ""

    sql = _render_sql(sql)

    conn = duckdb.connect(database=":memory:")
    _configure_s3(conn)
    conn.execute(sql)
    _print_result(conn)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
