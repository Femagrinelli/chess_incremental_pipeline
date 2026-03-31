from __future__ import annotations

import os
import traceback
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from duckdb_utils import escape_sql, list_sql_files, read_sql_file, run_sql


SQL_DIR = Path("/opt/airflow/sql")
SILVER_PLAYER_MONTH_GLOB = (
    "s3://{{S3_BUCKET}}/{{SILVER_PREFIX}}/"
    "player_month/year=*/month=*/title=*/bucket=*/part-000.parquet"
)
GOLD_ACTIVITY_GLOB = (
    "s3://{{S3_BUCKET}}/{{GOLD_PREFIX}}/"
    "title_month_activity/year=*/month=*/part-000.parquet"
)
GOLD_RATING_VOLATILITY_GLOB = (
    "s3://{{S3_BUCKET}}/{{GOLD_PREFIX}}/"
    "title_month_rating_volatility/year=*/month=*/part-000.parquet"
)
GOLD_COLOR_GLOB = (
    "s3://{{S3_BUCKET}}/{{GOLD_PREFIX}}/"
    "title_month_color_performance/year=*/month=*/part-000.parquet"
)
DEFAULT_SQL = f"""SELECT
    month_key,
    title,
    total_games,
    active_players,
    avg_games_per_active_player
FROM read_parquet(
    '{GOLD_ACTIVITY_GLOB}'
)
WHERE month_key BETWEEN '2026-01' AND '2026-03'
ORDER BY month_key, title
LIMIT 50;
"""


def _sql_list(values: list[str]) -> str:
    return ", ".join(f"'{escape_sql(value)}'" for value in values)


@st.cache_data(show_spinner=False, ttl=300)
def execute_query(sql: str) -> dict:
    conn, rendered_sql = run_sql(sql)
    if conn.description is None:
        return {
            "rendered_sql": rendered_sql,
            "row_count": 0,
            "dataframe": pd.DataFrame(),
        }

    dataframe = conn.fetch_df()
    return {
        "rendered_sql": rendered_sql,
        "row_count": len(dataframe.index),
        "dataframe": dataframe,
    }


def _safe_dataframe(sql: str) -> pd.DataFrame:
    try:
        return execute_query(sql)["dataframe"]
    except Exception:
        return pd.DataFrame()


@st.cache_data(show_spinner=False, ttl=300)
def load_available_months() -> list[str]:
    gold_df = _safe_dataframe(
        f"""
        SELECT DISTINCT month_key
        FROM read_parquet('{GOLD_ACTIVITY_GLOB}')
        ORDER BY month_key
        """
    )
    if not gold_df.empty:
        return gold_df["month_key"].dropna().astype(str).tolist()

    silver_df = _safe_dataframe(
        f"""
        SELECT DISTINCT month_key
        FROM read_parquet('{SILVER_PLAYER_MONTH_GLOB}')
        ORDER BY month_key
        """
    )
    if silver_df.empty:
        return []
    return silver_df["month_key"].dropna().astype(str).tolist()


@st.cache_data(show_spinner=False, ttl=300)
def load_available_titles() -> list[str]:
    gold_df = _safe_dataframe(
        f"""
        SELECT DISTINCT title
        FROM read_parquet('{GOLD_ACTIVITY_GLOB}')
        WHERE title IS NOT NULL
        ORDER BY title
        """
    )
    if not gold_df.empty:
        return gold_df["title"].dropna().astype(str).tolist()

    silver_df = _safe_dataframe(
        f"""
        SELECT DISTINCT title
        FROM read_parquet('{SILVER_PLAYER_MONTH_GLOB}')
        WHERE title IS NOT NULL
        ORDER BY title
        """
    )
    if silver_df.empty:
        return []
    return silver_df["title"].dropna().astype(str).tolist()


def build_activity_sql(start_month: str, end_month: str, titles: list[str]) -> str:
    return f"""
    SELECT
        month_key,
        title,
        total_games,
        active_players,
        avg_games_per_active_player,
        avg_unique_opponents,
        avg_player_win_rate,
        avg_player_draw_rate,
        avg_player_loss_rate,
        avg_player_rating
    FROM read_parquet('{GOLD_ACTIVITY_GLOB}')
    WHERE month_key BETWEEN '{escape_sql(start_month)}' AND '{escape_sql(end_month)}'
      AND title IN ({_sql_list(titles)})
    ORDER BY month_key, title
    """


def build_rating_volatility_sql(start_month: str, end_month: str, titles: list[str]) -> str:
    return f"""
    SELECT
        month_key,
        title,
        player_months,
        avg_games_per_player,
        avg_rating_range,
        median_rating_range,
        min_rating_range,
        max_rating_range,
        avg_rating_stddev,
        avg_player_rating
    FROM read_parquet('{GOLD_RATING_VOLATILITY_GLOB}')
    WHERE month_key BETWEEN '{escape_sql(start_month)}' AND '{escape_sql(end_month)}'
      AND title IN ({_sql_list(titles)})
    ORDER BY month_key, title
    """


def build_white_black_sql(start_month: str, end_month: str, titles: list[str]) -> str:
    return f"""
    SELECT
        title,
        SUM(white_games) AS white_games,
        ROUND(SUM(white_score_points) * 1.0 / NULLIF(SUM(white_games), 0), 4) AS white_score_rate,
        SUM(black_games) AS black_games,
        ROUND(SUM(black_score_points) * 1.0 / NULLIF(SUM(black_games), 0), 4) AS black_score_rate,
        ROUND(
            (SUM(white_score_points) * 1.0 / NULLIF(SUM(white_games), 0))
            - (SUM(black_score_points) * 1.0 / NULLIF(SUM(black_games), 0)),
            4
        ) AS white_minus_black,
        SUM(total_games) AS total_games
    FROM read_parquet('{GOLD_COLOR_GLOB}')
    WHERE month_key BETWEEN '{escape_sql(start_month)}' AND '{escape_sql(end_month)}'
      AND title IN ({_sql_list(titles)})
    GROUP BY 1
    ORDER BY white_minus_black DESC, title
    """


def build_white_black_trend_sql(start_month: str, end_month: str, titles: list[str]) -> str:
    return f"""
    SELECT
        month_key,
        title,
        white_score_rate,
        black_score_rate,
        white_minus_black,
        total_games
    FROM read_parquet('{GOLD_COLOR_GLOB}')
    WHERE month_key BETWEEN '{escape_sql(start_month)}' AND '{escape_sql(end_month)}'
      AND title IN ({_sql_list(titles)})
    ORDER BY month_key, title
    """


def download_csv_button(label: str, dataframe: pd.DataFrame, file_name: str) -> None:
    st.download_button(
        label,
        dataframe.to_csv(index=False).encode("utf-8"),
        file_name=file_name,
        mime="text/csv",
    )


st.set_page_config(page_title="Chess Gold Query UI", layout="wide")

st.title("Chess Gold Query UI")
st.caption("Pre-aggregated title-month marts in DuckDB over S3 parquet")

available_months = load_available_months()
available_titles = load_available_titles()

with st.sidebar:
    st.subheader("Context")
    st.write(f"S3 bucket: `{os.environ.get('S3_BUCKET', '')}`")
    st.write(f"Silver prefix: `{os.environ.get('SILVER_PREFIX', 'silver/chess_com')}`")
    st.write(f"Gold prefix: `{os.environ.get('GOLD_PREFIX', 'gold/chess_com')}`")
    st.write(f"AWS region: `{os.environ.get('AWS_REGION', 'us-east-1')}`")

    st.subheader("Dashboard Filters")
    if available_months:
        start_month = st.selectbox("Start month", available_months, index=max(len(available_months) - 3, 0))
        end_month = st.selectbox("End month", available_months, index=len(available_months) - 1)
    else:
        start_month = st.text_input("Start month", value="2026-01")
        end_month = st.text_input("End month", value="2026-03")

    default_titles = available_titles or ["GM", "IM", "FM"]
    selected_titles = st.multiselect("Titles", options=available_titles or default_titles, default=default_titles)

    st.subheader("SQL Templates")
    sample_files = list_sql_files(SQL_DIR)
    selected_file = st.selectbox("Sample query", [""] + sample_files)
    load_sample = st.button("Load sample")

    st.subheader("Tips")
    st.markdown(
        "- Dashboard tabs read from gold marts.\n"
        "- SQL Runner can query gold or silver.\n"
        "- If a dashboard is empty, run the gold DAGs first."
    )

if "sql_text" not in st.session_state:
    st.session_state.sql_text = DEFAULT_SQL

if load_sample and selected_file:
    st.session_state.sql_text = read_sql_file(SQL_DIR / selected_file)

dashboard_tab, volatility_tab, performance_tab, sql_tab = st.tabs(
    ["Activity Trends", "Rating Volatility", "White vs Black", "SQL Runner"]
)

if not selected_titles:
    st.warning("Select at least one title in the sidebar to render the dashboard.")
else:
    with dashboard_tab:
        st.subheader("Activity Trends")
        st.caption("Gold mart: monthly game volume, active players, and average games per active player.")
        try:
            activity_df = execute_query(build_activity_sql(start_month, end_month, selected_titles))["dataframe"]
            if activity_df.empty:
                st.info("No gold activity rows found. Run the gold backfill or current-month DAG first.")
            else:
                latest_month = activity_df["month_key"].max()
                latest_df = activity_df[activity_df["month_key"] == latest_month]

                metric_1, metric_2, metric_3 = st.columns(3)
                metric_1.metric("Latest month", latest_month)
                metric_2.metric("Games in latest month", f"{int(latest_df['total_games'].sum()):,}")
                metric_3.metric("Active titled players", f"{int(latest_df['active_players'].sum()):,}")

                st.altair_chart(
                    alt.Chart(activity_df)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("month_key:N", title="Month"),
                        y=alt.Y("total_games:Q", title="Total games"),
                        color=alt.Color("title:N", title="Title"),
                        tooltip=["month_key", "title", "total_games", "active_players", "avg_games_per_active_player"],
                    )
                    .properties(height=320),
                    use_container_width=True,
                )

                st.altair_chart(
                    alt.Chart(activity_df)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("month_key:N", title="Month"),
                        y=alt.Y("avg_games_per_active_player:Q", title="Avg games per active player"),
                        color=alt.Color("title:N", title="Title"),
                        tooltip=["month_key", "title", "avg_games_per_active_player", "avg_unique_opponents"],
                    )
                    .properties(height=320),
                    use_container_width=True,
                )

                st.dataframe(activity_df, use_container_width=True)
                download_csv_button("Download activity CSV", activity_df, "gold_activity_trends.csv")
        except Exception:
            st.error("Activity query failed.")
            st.code(traceback.format_exc())

    with volatility_tab:
        st.subheader("Rating Volatility By Title")
        st.caption("Gold mart: monthly volatility based on player-month rating range and standard deviation.")
        try:
            volatility_df = execute_query(build_rating_volatility_sql(start_month, end_month, selected_titles))["dataframe"]
            if volatility_df.empty:
                st.info("No gold volatility rows found. Run the gold backfill or current-month DAG first.")
            else:
                st.altair_chart(
                    alt.Chart(volatility_df)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("month_key:N", title="Month"),
                        y=alt.Y("avg_rating_range:Q", title="Avg rating range"),
                        color=alt.Color("title:N", title="Title"),
                        tooltip=["month_key", "title", "avg_rating_range", "median_rating_range", "player_months"],
                    )
                    .properties(height=320),
                    use_container_width=True,
                )

                st.altair_chart(
                    alt.Chart(volatility_df)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("month_key:N", title="Month"),
                        y=alt.Y("avg_rating_stddev:Q", title="Avg rating stddev"),
                        color=alt.Color("title:N", title="Title"),
                        tooltip=["month_key", "title", "avg_rating_stddev", "avg_player_rating"],
                    )
                    .properties(height=320),
                    use_container_width=True,
                )

                st.dataframe(volatility_df, use_container_width=True)
                download_csv_button("Download volatility CSV", volatility_df, "gold_rating_volatility.csv")
        except Exception:
            st.error("Volatility query failed.")
            st.code(traceback.format_exc())

    with performance_tab:
        st.subheader("White Vs Black Performance By Title")
        st.caption("Gold mart: weighted score rate with white and black across the selected date range.")
        try:
            white_black_df = execute_query(build_white_black_sql(start_month, end_month, selected_titles))["dataframe"]
            white_black_trend_df = execute_query(build_white_black_trend_sql(start_month, end_month, selected_titles))["dataframe"]

            if white_black_df.empty:
                st.info("No gold white-vs-black rows found. Run the gold backfill or current-month DAG first.")
            else:
                comparison_df = white_black_df.melt(
                    id_vars=["title", "white_games", "black_games", "white_minus_black", "total_games"],
                    value_vars=["white_score_rate", "black_score_rate"],
                    var_name="metric",
                    value_name="score_rate",
                )
                comparison_df["metric"] = comparison_df["metric"].map(
                    {
                        "white_score_rate": "White score rate",
                        "black_score_rate": "Black score rate",
                    }
                )

                st.altair_chart(
                    alt.Chart(comparison_df)
                    .mark_bar()
                    .encode(
                        x=alt.X("title:N", title="Title"),
                        y=alt.Y("score_rate:Q", title="Score rate"),
                        color=alt.Color("metric:N", title="Color performance"),
                        xOffset="metric:N",
                        tooltip=["title", "metric", "score_rate"],
                    )
                    .properties(height=320),
                    use_container_width=True,
                )

                if not white_black_trend_df.empty:
                    st.altair_chart(
                        alt.Chart(white_black_trend_df)
                        .mark_line(point=True)
                        .encode(
                            x=alt.X("month_key:N", title="Month"),
                            y=alt.Y("white_minus_black:Q", title="White minus black score rate"),
                            color=alt.Color("title:N", title="Title"),
                            tooltip=["month_key", "title", "white_score_rate", "black_score_rate", "white_minus_black"],
                        )
                        .properties(height=320),
                        use_container_width=True,
                    )

                st.dataframe(white_black_df, use_container_width=True)
                download_csv_button("Download white-vs-black CSV", white_black_df, "gold_white_vs_black.csv")
        except Exception:
            st.error("White-vs-black query failed.")
            st.code(traceback.format_exc())

with sql_tab:
    st.subheader("SQL Runner")
    sql_text = st.text_area("SQL", value=st.session_state.sql_text, height=320)
    st.session_state.sql_text = sql_text

    left, right = st.columns([1, 4])
    run_clicked = left.button("Run Query", type="primary")
    show_sql = right.checkbox("Show rendered SQL", value=False)

    if run_clicked:
        try:
            with st.spinner("Running query..."):
                result = execute_query(sql_text)

            st.success(f"Query finished. Rows returned: {result['row_count']}")
            if show_sql:
                st.code(result["rendered_sql"], language="sql")

            dataframe = result["dataframe"]
            if not dataframe.empty:
                st.dataframe(dataframe, use_container_width=True)
                download_csv_button("Download query CSV", dataframe, "duckdb_query_result.csv")
            else:
                st.info("Query executed successfully and did not return rows.")
        except Exception:
            st.error("Query failed.")
            st.code(traceback.format_exc())
