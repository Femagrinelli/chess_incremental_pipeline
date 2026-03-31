from __future__ import annotations

import os
import traceback
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from duckdb_utils import escape_sql, list_sql_files, read_sql_file, run_sql


SQL_DIR = Path("/opt/airflow/sql")
PLAYER_MONTH_GLOB = (
    "s3://{{S3_BUCKET}}/{{SILVER_PREFIX}}/"
    "player_month/year=*/month=*/title=*/bucket=*/part-000.parquet"
)
DEFAULT_SQL = f"""SELECT
    title,
    SUM(games_played) AS total_games,
    ROUND(AVG(games_played), 2) AS avg_games_per_player
FROM read_parquet(
    '{PLAYER_MONTH_GLOB}'
)
WHERE month_key BETWEEN '2026-01' AND '2026-03'
GROUP BY 1
ORDER BY total_games DESC
LIMIT 20;
"""


def _sql_list(values: list[str]) -> str:
    return ", ".join(f"'{escape_sql(value)}'" for value in values)


def _base_player_month_sql() -> str:
    return f"read_parquet('{PLAYER_MONTH_GLOB}')"


@st.cache_data(show_spinner=False, ttl=300)
def execute_query(sql: str) -> dict:
    conn, rendered_sql = run_sql(sql)
    if conn.description is None:
        return {
            "rendered_sql": rendered_sql,
            "columns": [],
            "rows": [],
            "row_count": 0,
            "dataframe": pd.DataFrame(),
        }

    dataframe = conn.fetch_df()
    return {
        "rendered_sql": rendered_sql,
        "columns": list(dataframe.columns),
        "rows": dataframe.to_dict(orient="records"),
        "row_count": len(dataframe.index),
        "dataframe": dataframe,
    }


@st.cache_data(show_spinner=False, ttl=300)
def load_available_months() -> list[str]:
    result = execute_query(
        f"""
        SELECT DISTINCT month_key
        FROM {_base_player_month_sql()}
        ORDER BY month_key
        """
    )
    dataframe = result["dataframe"]
    if dataframe.empty:
        return []
    return dataframe["month_key"].dropna().astype(str).tolist()


@st.cache_data(show_spinner=False, ttl=300)
def load_available_titles() -> list[str]:
    result = execute_query(
        f"""
        SELECT DISTINCT title
        FROM {_base_player_month_sql()}
        WHERE title IS NOT NULL
        ORDER BY title
        """
    )
    dataframe = result["dataframe"]
    if dataframe.empty:
        return []
    return dataframe["title"].dropna().astype(str).tolist()


def build_activity_sql(start_month: str, end_month: str, titles: list[str]) -> str:
    return f"""
    SELECT
        month_key,
        title,
        SUM(games_played) AS total_games,
        COUNT(DISTINCT username) AS active_players,
        ROUND(SUM(games_played) * 1.0 / NULLIF(COUNT(DISTINCT username), 0), 2) AS avg_games_per_active_player,
        ROUND(AVG(win_rate), 4) AS avg_player_win_rate,
        ROUND(AVG(unique_opponent_count), 2) AS avg_unique_opponents
    FROM {_base_player_month_sql()}
    WHERE month_key BETWEEN '{escape_sql(start_month)}' AND '{escape_sql(end_month)}'
      AND title IN ({_sql_list(titles)})
    GROUP BY 1, 2
    ORDER BY 1, 2
    """


def build_rating_volatility_sql(start_month: str, end_month: str, titles: list[str]) -> str:
    return f"""
    SELECT
        month_key,
        title,
        COUNT(*) AS player_months,
        ROUND(AVG(rating_range), 2) AS avg_rating_range,
        ROUND(MEDIAN(rating_range), 2) AS median_rating_range,
        ROUND(AVG(rating_stddev), 2) AS avg_rating_stddev,
        ROUND(AVG(avg_rating), 2) AS avg_player_rating
    FROM {_base_player_month_sql()}
    WHERE month_key BETWEEN '{escape_sql(start_month)}' AND '{escape_sql(end_month)}'
      AND title IN ({_sql_list(titles)})
      AND games_played >= 2
      AND rating_range IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 1, 2
    """


def build_white_black_sql(start_month: str, end_month: str, titles: list[str]) -> str:
    return f"""
    SELECT
        title,
        SUM(white_games) AS white_games,
        ROUND(SUM(white_score) * 1.0 / NULLIF(SUM(white_games), 0), 4) AS white_score_rate,
        SUM(black_games) AS black_games,
        ROUND(SUM(black_score) * 1.0 / NULLIF(SUM(black_games), 0), 4) AS black_score_rate,
        ROUND(
            (SUM(white_score) * 1.0 / NULLIF(SUM(white_games), 0))
            - (SUM(black_score) * 1.0 / NULLIF(SUM(black_games), 0)),
            4
        ) AS white_minus_black
    FROM {_base_player_month_sql()}
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
        ROUND(SUM(white_score) * 1.0 / NULLIF(SUM(white_games), 0), 4) AS white_score_rate,
        ROUND(SUM(black_score) * 1.0 / NULLIF(SUM(black_games), 0), 4) AS black_score_rate,
        ROUND(
            (SUM(white_score) * 1.0 / NULLIF(SUM(white_games), 0))
            - (SUM(black_score) * 1.0 / NULLIF(SUM(black_games), 0)),
            4
        ) AS white_minus_black
    FROM {_base_player_month_sql()}
    WHERE month_key BETWEEN '{escape_sql(start_month)}' AND '{escape_sql(end_month)}'
      AND title IN ({_sql_list(titles)})
    GROUP BY 1, 2
    ORDER BY 1, 2
    """


def download_csv_button(label: str, dataframe: pd.DataFrame, file_name: str) -> None:
    st.download_button(
        label,
        dataframe.to_csv(index=False).encode("utf-8"),
        file_name=file_name,
        mime="text/csv",
    )


st.set_page_config(page_title="Chess Silver Query UI", layout="wide")

st.title("Chess Silver Query UI")
st.caption("DuckDB over S3 parquet for the silver layer")

available_months = load_available_months()
available_titles = load_available_titles()

with st.sidebar:
    st.subheader("Context")
    st.write(f"S3 bucket: `{os.environ.get('S3_BUCKET', '')}`")
    st.write(f"Silver prefix: `{os.environ.get('SILVER_PREFIX', 'silver/chess_com')}`")
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
        "- Use the dashboard tabs for the common analyses.\n"
        "- Use `player_month` first for faster trend work.\n"
        "- Keep `LIMIT` in ad hoc SQL while exploring."
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
        st.caption("Monthly game volume, active players, and average games per active player.")
        try:
            activity_result = execute_query(build_activity_sql(start_month, end_month, selected_titles))
            activity_df = activity_result["dataframe"]
            if activity_df.empty:
                st.info("No activity rows found for the selected range.")
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
                download_csv_button("Download activity CSV", activity_df, "activity_trends.csv")
        except Exception:
            st.error("Activity query failed.")
            st.code(traceback.format_exc())

    with volatility_tab:
        st.subheader("Rating Volatility By Title")
        st.caption("Monthly volatility based on each player-month rating range and rating standard deviation.")
        try:
            volatility_result = execute_query(build_rating_volatility_sql(start_month, end_month, selected_titles))
            volatility_df = volatility_result["dataframe"]
            if volatility_df.empty:
                st.info("No volatility rows found for the selected range.")
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
                download_csv_button("Download volatility CSV", volatility_df, "rating_volatility.csv")
        except Exception:
            st.error("Volatility query failed.")
            st.code(traceback.format_exc())

    with performance_tab:
        st.subheader("White Vs Black Performance By Title")
        st.caption("Weighted score rate with white and black across the selected date range.")
        try:
            white_black_result = execute_query(build_white_black_sql(start_month, end_month, selected_titles))
            white_black_df = white_black_result["dataframe"]
            white_black_trend_result = execute_query(build_white_black_trend_sql(start_month, end_month, selected_titles))
            white_black_trend_df = white_black_trend_result["dataframe"]

            if white_black_df.empty:
                st.info("No white-vs-black rows found for the selected range.")
            else:
                comparison_df = white_black_df.melt(
                    id_vars=["title", "white_games", "black_games", "white_minus_black"],
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
                download_csv_button("Download white-vs-black CSV", white_black_df, "white_vs_black.csv")
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
