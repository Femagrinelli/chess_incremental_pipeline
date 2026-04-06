from __future__ import annotations

import json
import math
import shutil

import pandas as pd


DEFAULT_MAX_ROWS = 100
DEFAULT_MAX_COL_WIDTH = 32


def _stringify(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, float) and math.isnan(value):
        return "NULL"
    text = str(value).replace("\n", " ").replace("\r", " ")
    return " ".join(text.split())


def _truncate(value: str, max_width: int) -> str:
    if max_width <= 1 or len(value) <= max_width:
        return value
    if max_width <= 3:
        return value[:max_width]
    return value[: max_width - 3] + "..."


def _row_count_message(total_rows: int, shown_rows: int) -> str:
    if total_rows == shown_rows:
        return f"({total_rows} rows)"
    return f"(showing first {shown_rows} of {total_rows} rows)"


def render_table(df: pd.DataFrame, max_rows: int = DEFAULT_MAX_ROWS, max_col_width: int = DEFAULT_MAX_COL_WIDTH) -> str:
    if df.empty:
        return "(0 rows)"

    shown = df.head(max_rows).copy()
    terminal_width = shutil.get_terminal_size((140, 24)).columns
    safe_width = max(12, min(max_col_width, max(12, terminal_width // max(len(shown.columns), 1) - 3)))

    for column in shown.columns:
        shown[column] = shown[column].map(lambda value: _truncate(_stringify(value), safe_width))

    body = shown.to_string(index=False)
    return f"{body}\n{_row_count_message(len(df), len(shown))}"


def render_vertical(df: pd.DataFrame, max_rows: int = DEFAULT_MAX_ROWS, max_col_width: int = 120) -> str:
    if df.empty:
        return "(0 rows)"

    shown = df.head(max_rows)
    lines: list[str] = []
    for index, (_, row) in enumerate(shown.iterrows(), start=1):
        lines.append(f"-[ RECORD {index} ]" + "-" * 40)
        for column in shown.columns:
            value = _truncate(_stringify(row[column]), max_col_width)
            lines.append(f"{column}: {value}")
    lines.append(_row_count_message(len(df), len(shown)))
    return "\n".join(lines)


def render_json(df: pd.DataFrame, max_rows: int = DEFAULT_MAX_ROWS) -> str:
    shown = df.head(max_rows)
    payload = shown.to_dict(orient="records")
    body = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    return f"{body}\n{_row_count_message(len(df), len(shown))}"


def render_dataframe(
    df: pd.DataFrame,
    output_format: str = "table",
    max_rows: int = DEFAULT_MAX_ROWS,
    max_col_width: int = DEFAULT_MAX_COL_WIDTH,
) -> str:
    if output_format == "vertical":
        return render_vertical(df, max_rows=max_rows, max_col_width=max_col_width)
    if output_format == "json":
        return render_json(df, max_rows=max_rows)
    return render_table(df, max_rows=max_rows, max_col_width=max_col_width)
