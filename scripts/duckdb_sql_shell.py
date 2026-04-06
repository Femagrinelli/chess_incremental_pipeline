from __future__ import annotations

import sys

import duckdb

from duckdb_query_service import create_connection
from duckdb_output import render_dataframe


PROMPT = "duckdb> "
CONTINUATION_PROMPT = "   ...> "


def _print_result(cursor: duckdb.DuckDBPyConnection) -> None:
    if cursor.description is None:
        print("OK")
        return

    df = cursor.fetchdf()
    print(
        render_dataframe(
            df,
            output_format=main.output_format,
            max_rows=main.max_rows,
            max_col_width=main.max_col_width,
        )
    )


def main() -> int:
    conn = create_connection()
    buffer: list[str] = []
    main.output_format = "table"
    main.max_rows = 100
    main.max_col_width = 32

    print("DuckDB SQL shell")
    print("Connected to S3-backed parquet using the container AWS environment.")
    print("End each query with ';' and use .quit to exit.")
    print("Commands: .mode table | .mode vertical | .mode json | .limit N | .width N")

    try:
        while True:
            prompt = PROMPT if not buffer else CONTINUATION_PROMPT
            try:
                line = input(prompt)
            except EOFError:
                print("")
                break

            stripped = line.strip()
            if not buffer and stripped in {".quit", ".exit"}:
                break
            if not buffer and stripped.startswith(".mode "):
                mode = stripped.split(maxsplit=1)[1].strip().lower()
                if mode in {"table", "vertical", "json"}:
                    main.output_format = mode
                    print(f"Output mode set to {mode}.")
                else:
                    print("ERROR: mode must be one of table, vertical, json", file=sys.stderr)
                continue
            if not buffer and stripped.startswith(".limit "):
                try:
                    main.max_rows = max(1, int(stripped.split(maxsplit=1)[1].strip()))
                    print(f"Row display limit set to {main.max_rows}.")
                except ValueError:
                    print("ERROR: .limit expects an integer", file=sys.stderr)
                continue
            if not buffer and stripped.startswith(".width "):
                try:
                    main.max_col_width = max(8, int(stripped.split(maxsplit=1)[1].strip()))
                    print(f"Column width set to {main.max_col_width}.")
                except ValueError:
                    print("ERROR: .width expects an integer", file=sys.stderr)
                continue
            if not buffer and not stripped:
                continue

            buffer.append(line)
            if ";" not in line:
                continue

            sql = "\n".join(buffer).strip()
            buffer.clear()

            try:
                cursor = conn.execute(sql)
                _print_result(cursor)
            except Exception as exc:  # pragma: no cover - interactive helper
                print(f"ERROR: {exc}", file=sys.stderr)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
