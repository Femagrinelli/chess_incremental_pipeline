#!/usr/bin/env python3
"""
Small helper to run DuckDB queries against S3-backed silver parquet datasets.
"""

from __future__ import annotations

import argparse
from duckdb_utils import read_sql_file, run_sql


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
        sql = read_sql_file(args.file)
    else:
        sql = args.sql or ""

    conn, _ = run_sql(sql)
    _print_result(conn)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
