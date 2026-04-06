from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from duckdb_query_service import create_connection
from duckdb_output import render_dataframe


def _print_result(
    cursor: duckdb.DuckDBPyConnection,
    output_format: str,
    max_rows: int,
    max_col_width: int,
) -> None:
    if cursor.description is None:
        print("OK")
        return

    df = cursor.fetchdf()
    print(render_dataframe(df, output_format=output_format, max_rows=max_rows, max_col_width=max_col_width))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a SQL file with the S3-backed DuckDB connection.")
    parser.add_argument("sql_file", help="Path to the SQL file inside the container, for example /opt/airflow/sql/query.sql")
    parser.add_argument("--format", choices=["table", "vertical", "json"], default="table")
    parser.add_argument("--max-rows", type=int, default=100)
    parser.add_argument("--max-col-width", type=int, default=32)
    args = parser.parse_args()

    sql_path = Path(args.sql_file)
    sql = sql_path.read_text(encoding="utf-8").strip()
    if not sql:
        raise SystemExit(f"SQL file is empty: {sql_path}")

    conn = create_connection()
    try:
        cursor = conn.execute(sql)
        _print_result(
            cursor,
            output_format=args.format,
            max_rows=args.max_rows,
            max_col_width=args.max_col_width,
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
