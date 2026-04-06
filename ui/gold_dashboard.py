from __future__ import annotations

import pandas as pd
import streamlit as st

from duckdb_query_service import (
    escape_sql_literal,
    gold_prefix,
    normalize_pair,
    run_query,
    s3_bucket,
    silver_prefix,
    sql_in_list,
)

st.set_page_config(page_title="Chess Gold Dashboard", layout="wide")

GOLD_ACTIVITY_GLOB = f"s3://{s3_bucket()}/{gold_prefix()}/title_month_activity/year=*/month=*/part.parquet"
GOLD_VOLATILITY_GLOB = f"s3://{s3_bucket()}/{gold_prefix()}/title_month_rating_volatility/year=*/month=*/part.parquet"
GOLD_COLOR_GLOB = f"s3://{s3_bucket()}/{gold_prefix()}/title_month_color_performance/year=*/month=*/part.parquet"
GOLD_H2H_SUMMARY = f"s3://{s3_bucket()}/{gold_prefix()}/head_to_head_summary/part.parquet"
GOLD_H2H_GAMES_GLOB = f"s3://{s3_bucket()}/{gold_prefix()}/head_to_head_games/year=*/month=*/part.parquet"
SILVER_ROSTER_GLOB = f"s3://{s3_bucket()}/{silver_prefix()}/roster_daily/snapshot_date=*/part.parquet"
SILVER_PLAYER_GAMES_GLOB = f"s3://{s3_bucket()}/{silver_prefix()}/player_game_facts/year=*/month=*/part.parquet"


@st.cache_data(ttl=900, show_spinner=False)
def query_dataframe(sql: str) -> pd.DataFrame:
    return run_query(sql)


@st.cache_data(ttl=1800, show_spinner=False)
def available_months() -> list[str]:
    sql = f"""
    SELECT DISTINCT month_key
    FROM read_parquet('{GOLD_ACTIVITY_GLOB}', hive_partitioning=true)
    ORDER BY month_key
    """
    df = query_dataframe(sql)
    if df.empty:
        return []
    return [str(value) for value in df["month_key"].dropna().tolist()]


@st.cache_data(ttl=1800, show_spinner=False)
def available_titles() -> list[str]:
    sql = f"""
    SELECT DISTINCT title
    FROM read_parquet('{GOLD_ACTIVITY_GLOB}', hive_partitioning=true)
    ORDER BY title
    """
    df = query_dataframe(sql)
    if df.empty:
        return []
    return [str(value) for value in df["title"].dropna().tolist()]


@st.cache_data(ttl=3600, show_spinner=False)
def available_players() -> pd.DataFrame:
    sql = f"""
    SELECT
      lower(username) AS username,
      min(title) AS title,
      max(snapshot_date) AS latest_snapshot_date
    FROM read_parquet('{SILVER_ROSTER_GLOB}', hive_partitioning=true)
    GROUP BY 1
    ORDER BY 1
    """
    return query_dataframe(sql)


def _filter_clause(start_month: str, end_month: str, titles: list[str]) -> str:
    title_filter = ""
    if titles:
        title_filter = f"AND title IN ({sql_in_list(titles)})"

    return f"""
    WHERE month_key >= '{escape_sql_literal(start_month)}'
      AND month_key <= '{escape_sql_literal(end_month)}'
      {title_filter}
    """


def _player_filter_clause(username: str, month_key: str | None = None, game_date: str | None = None) -> str:
    filters = [f"lower(username) = '{escape_sql_literal(username.lower())}'"]
    if month_key:
        filters.append(f"month_key = '{escape_sql_literal(month_key)}'")
    if game_date:
        filters.append(f"CAST(game_date AS varchar) = '{escape_sql_literal(game_date)}'")
    return "WHERE " + "\n      AND ".join(filters)


@st.cache_data(ttl=900, show_spinner=False)
def load_activity(start_month: str, end_month: str, titles: tuple[str, ...]) -> pd.DataFrame:
    sql = f"""
    SELECT
      month_key,
      title,
      CAST(active_players AS bigint) AS active_players,
      CAST(total_games AS bigint) AS total_games,
      CAST(avg_games_per_active_player AS double) AS avg_games_per_active_player,
      CAST(avg_unique_opponents AS double) AS avg_unique_opponents,
      CAST(avg_player_score_rate AS double) AS avg_player_score_rate,
      CAST(avg_player_rating AS double) AS avg_player_rating
    FROM read_parquet('{GOLD_ACTIVITY_GLOB}', hive_partitioning=true)
    {_filter_clause(start_month, end_month, list(titles))}
    ORDER BY month_key, title
    """
    return query_dataframe(sql)


@st.cache_data(ttl=900, show_spinner=False)
def load_volatility(start_month: str, end_month: str, titles: tuple[str, ...]) -> pd.DataFrame:
    sql = f"""
    SELECT
      month_key,
      title,
      CAST(player_months AS bigint) AS player_months,
      CAST(avg_games_per_player AS double) AS avg_games_per_player,
      CAST(avg_rating_range AS double) AS avg_rating_range,
      CAST(median_rating_range AS double) AS median_rating_range,
      CAST(avg_rating_stddev AS double) AS avg_rating_stddev,
      CAST(avg_player_rating AS double) AS avg_player_rating
    FROM read_parquet('{GOLD_VOLATILITY_GLOB}', hive_partitioning=true)
    {_filter_clause(start_month, end_month, list(titles))}
    ORDER BY month_key, title
    """
    return query_dataframe(sql)


@st.cache_data(ttl=900, show_spinner=False)
def load_color_performance(start_month: str, end_month: str, titles: tuple[str, ...]) -> pd.DataFrame:
    sql = f"""
    SELECT
      month_key,
      title,
      CAST(white_games AS bigint) AS white_games,
      CAST(white_score_rate AS double) AS white_score_rate,
      CAST(black_games AS bigint) AS black_games,
      CAST(black_score_rate AS double) AS black_score_rate,
      CAST(white_minus_black AS double) AS white_minus_black,
      CAST(total_games AS bigint) AS total_games
    FROM read_parquet('{GOLD_COLOR_GLOB}', hive_partitioning=true)
    {_filter_clause(start_month, end_month, list(titles))}
    ORDER BY month_key, title
    """
    return query_dataframe(sql)


@st.cache_data(ttl=900, show_spinner=False)
def load_head_to_head_summary(player_one: str, player_two: str) -> pd.DataFrame:
    left, right = normalize_pair(player_one, player_two)
    sql = f"""
    SELECT
      player_a,
      player_b,
      CAST(games_played AS bigint) AS games_played,
      CAST(player_a_wins AS bigint) AS player_a_wins,
      CAST(player_b_wins AS bigint) AS player_b_wins,
      CAST(draws AS bigint) AS draws,
      CAST(rated_games AS bigint) AS rated_games,
      first_game_date,
      last_game_date,
      CAST(player_a_win_rate AS double) AS player_a_win_rate,
      CAST(player_b_win_rate AS double) AS player_b_win_rate,
      CAST(draw_rate AS double) AS draw_rate
    FROM read_parquet('{GOLD_H2H_SUMMARY}')
    WHERE lower(player_a) = '{escape_sql_literal(left)}'
      AND lower(player_b) = '{escape_sql_literal(right)}'
    """
    return query_dataframe(sql)


@st.cache_data(ttl=900, show_spinner=False)
def load_head_to_head_games(player_one: str, player_two: str) -> pd.DataFrame:
    left, right = normalize_pair(player_one, player_two)
    sql = f"""
    SELECT
      game_date,
      time_class,
      rated,
      opening_name,
      white_username,
      black_username,
      winner_username,
      player_a,
      player_b,
      player_a_color,
      player_a_score,
      player_b_score,
      game_url
    FROM read_parquet('{GOLD_H2H_GAMES_GLOB}', hive_partitioning=true)
    WHERE lower(player_a) = '{escape_sql_literal(left)}'
      AND lower(player_b) = '{escape_sql_literal(right)}'
    ORDER BY game_date DESC, game_url DESC
    """
    return query_dataframe(sql)


@st.cache_data(ttl=1800, show_spinner=False)
def player_months(username: str) -> list[str]:
    sql = f"""
    SELECT DISTINCT month_key
    FROM read_parquet('{SILVER_PLAYER_GAMES_GLOB}', hive_partitioning=true)
    {_player_filter_clause(username)}
    ORDER BY month_key DESC
    """
    df = query_dataframe(sql)
    if df.empty:
        return []
    return [str(value) for value in df["month_key"].dropna().tolist()]


@st.cache_data(ttl=1800, show_spinner=False)
def player_days(username: str, month_key: str) -> list[str]:
    sql = f"""
    SELECT DISTINCT CAST(game_date AS varchar) AS game_date
    FROM read_parquet('{SILVER_PLAYER_GAMES_GLOB}', hive_partitioning=true)
    {_player_filter_clause(username, month_key=month_key)}
    ORDER BY game_date DESC
    """
    df = query_dataframe(sql)
    if df.empty:
        return []
    return [str(value) for value in df["game_date"].dropna().tolist()]


@st.cache_data(ttl=900, show_spinner=False)
def load_player_summary(username: str, month_key: str, game_date: str | None = None) -> pd.DataFrame:
    sql = f"""
    SELECT
      min(title) AS title,
      CAST(count(*) AS bigint) AS games_played,
      CAST(sum(CASE WHEN score = 1.0 THEN 1 ELSE 0 END) AS bigint) AS wins,
      CAST(sum(CASE WHEN score = 0.5 THEN 1 ELSE 0 END) AS bigint) AS draws,
      CAST(sum(CASE WHEN score = 0.0 THEN 1 ELSE 0 END) AS bigint) AS losses,
      CAST(avg(score) AS double) AS avg_score,
      CAST(avg(rating) AS double) AS avg_rating,
      CAST(avg(rating_diff) AS double) AS avg_rating_diff,
      CAST(count(DISTINCT opponent_username) AS bigint) AS unique_opponents,
      min(game_date) AS first_game_date,
      max(game_date) AS last_game_date
    FROM read_parquet('{SILVER_PLAYER_GAMES_GLOB}', hive_partitioning=true)
    {_player_filter_clause(username, month_key=month_key, game_date=game_date)}
    """
    return query_dataframe(sql)


@st.cache_data(ttl=900, show_spinner=False)
def load_player_daily_counts(username: str, month_key: str) -> pd.DataFrame:
    sql = f"""
    SELECT
      CAST(game_date AS varchar) AS game_date,
      CAST(count(*) AS bigint) AS games_played
    FROM read_parquet('{SILVER_PLAYER_GAMES_GLOB}', hive_partitioning=true)
    {_player_filter_clause(username, month_key=month_key)}
    GROUP BY 1
    ORDER BY game_date
    """
    return query_dataframe(sql)


@st.cache_data(ttl=900, show_spinner=False)
def load_player_games(username: str, month_key: str, game_date: str | None = None) -> pd.DataFrame:
    sql = f"""
    SELECT
      CAST(game_date AS varchar) AS game_date,
      time_class,
      rated,
      color,
      opponent_username,
      opponent_title,
      CAST(rating AS bigint) AS rating,
      CAST(opponent_rating AS bigint) AS opponent_rating,
      CAST(rating_diff AS bigint) AS rating_diff,
      result,
      CAST(score AS double) AS score,
      opening_name,
      game_url
    FROM read_parquet('{SILVER_PLAYER_GAMES_GLOB}', hive_partitioning=true)
    {_player_filter_clause(username, month_key=month_key, game_date=game_date)}
    ORDER BY game_date DESC, game_url DESC
    """
    return query_dataframe(sql)


def _cast_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = df.copy()
    for column in columns:
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    return result


st.title("Chess Gold Dashboard")
st.caption("Streamlit dashboard backed by DuckDB queries over dbt gold and silver parquet in S3.")

with st.sidebar:
    st.subheader("Environment")
    st.write(f"S3 bucket: `{s3_bucket()}`")
    st.write(f"Gold prefix: `{gold_prefix()}`")
    st.write(f"Silver prefix: `{silver_prefix()}`")

    st.subheader("Filters")
    months = available_months()
    titles = available_titles()

    if months:
        default_start = months[max(len(months) - 12, 0)]
        default_end = months[-1]
        start_month = st.selectbox("Start month", options=months, index=months.index(default_start))
        end_month = st.selectbox("End month", options=months, index=months.index(default_end))
    else:
        start_month = st.text_input("Start month", value="")
        end_month = st.text_input("End month", value="")

    selected_titles = st.multiselect("Titles", options=titles, default=titles)

    if st.button("Clear query cache"):
        st.cache_data.clear()
        st.success("Cache cleared.")

tab_activity, tab_volatility, tab_color, tab_h2h, tab_player = st.tabs(
    ["Activity Trends", "Rating Volatility", "White vs Black", "Head to Head", "Player Explorer"]
)

with tab_activity:
    st.subheader("Title Activity Trends")
    if not start_month or not end_month:
        st.info("No gold month partitions were found yet. Build the gold layer first.")
    else:
        activity_df = _cast_numeric(
            load_activity(start_month, end_month, tuple(selected_titles)),
            ["active_players", "total_games", "avg_games_per_active_player", "avg_unique_opponents", "avg_player_score_rate", "avg_player_rating"],
        )
        st.dataframe(activity_df, use_container_width=True)
        if not activity_df.empty:
            pivot = activity_df.pivot(index="month_key", columns="title", values="total_games").fillna(0)
            st.line_chart(pivot)

with tab_volatility:
    st.subheader("Rating Volatility by Title")
    if not start_month or not end_month:
        st.info("No gold month partitions were found yet. Build the gold layer first.")
    else:
        volatility_df = _cast_numeric(
            load_volatility(start_month, end_month, tuple(selected_titles)),
            ["player_months", "avg_games_per_player", "avg_rating_range", "median_rating_range", "avg_rating_stddev", "avg_player_rating"],
        )
        st.dataframe(volatility_df, use_container_width=True)
        if not volatility_df.empty:
            pivot = volatility_df.pivot(index="month_key", columns="title", values="avg_rating_range").fillna(0)
            st.line_chart(pivot)

with tab_color:
    st.subheader("White vs Black Performance")
    if not start_month or not end_month:
        st.info("No gold month partitions were found yet. Build the gold layer first.")
    else:
        color_df = _cast_numeric(
            load_color_performance(start_month, end_month, tuple(selected_titles)),
            ["white_games", "white_score_rate", "black_games", "black_score_rate", "white_minus_black", "total_games"],
        )
        st.dataframe(color_df, use_container_width=True)
        if not color_df.empty:
            pivot = color_df.pivot(index="month_key", columns="title", values="white_minus_black").fillna(0)
            st.line_chart(pivot)

with tab_h2h:
    st.subheader("Head-to-Head Search")
    col1, col2 = st.columns(2)
    with col1:
        player_one = st.text_input("Player one", value="")
    with col2:
        player_two = st.text_input("Player two", value="")

    if player_one and player_two:
        summary_df = _cast_numeric(
            load_head_to_head_summary(player_one, player_two),
            ["games_played", "player_a_wins", "player_b_wins", "draws", "rated_games", "player_a_win_rate", "player_b_win_rate", "draw_rate"],
        )
        if summary_df.empty:
            st.info("No head-to-head games found for that player pair in the gold layer.")
        else:
            st.dataframe(summary_df, use_container_width=True)
            games_df = _cast_numeric(
                load_head_to_head_games(player_one, player_two),
                ["player_a_score", "player_b_score"],
            )
            st.dataframe(games_df, use_container_width=True)
    else:
        st.caption("Enter two usernames to search the gold head-to-head marts.")

with tab_player:
    st.subheader("Player Explorer")
    players_df = available_players()
    if players_df.empty:
        st.info("No silver roster data was found yet. Build the silver layer first.")
    else:
        player_lookup = {
            row["username"]: (row["title"] if pd.notna(row["title"]) else "")
            for _, row in players_df.iterrows()
        }
        selected_player = st.selectbox(
            "Player",
            options=players_df["username"].tolist(),
            index=None,
            placeholder="Search for a titled player",
            format_func=lambda username: f"{username} ({player_lookup.get(username, '')})" if player_lookup.get(username) else username,
        )

        if selected_player:
            months_for_player = player_months(selected_player)
            if not months_for_player:
                st.info("No silver player-game facts were found for this player yet.")
            else:
                view_mode = st.radio("View", options=["Month", "Day"], horizontal=True)
                selected_month = st.selectbox("Month", options=months_for_player)
                selected_day = None

                if view_mode == "Day":
                    days_for_player = player_days(selected_player, selected_month)
                    if not days_for_player:
                        st.info("No game days were found for that player and month.")
                    else:
                        selected_day = st.selectbox("Day", options=days_for_player)

                if view_mode == "Month" or selected_day:
                    summary_df = _cast_numeric(
                        load_player_summary(selected_player, selected_month, selected_day),
                        ["games_played", "wins", "draws", "losses", "avg_score", "avg_rating", "avg_rating_diff", "unique_opponents"],
                    )

                    if not summary_df.empty:
                        summary = summary_df.iloc[0]
                        metric_cols = st.columns(5)
                        metric_cols[0].metric("Games", int(summary["games_played"]))
                        metric_cols[1].metric("Wins", int(summary["wins"]))
                        metric_cols[2].metric("Draws", int(summary["draws"]))
                        metric_cols[3].metric("Losses", int(summary["losses"]))
                        metric_cols[4].metric("Avg Score", f"{summary['avg_score']:.3f}")

                        detail_cols = st.columns(4)
                        detail_cols[0].metric("Title", str(summary["title"]))
                        detail_cols[1].metric("Avg Rating", f"{summary['avg_rating']:.0f}")
                        detail_cols[2].metric("Avg Rating Diff", f"{summary['avg_rating_diff']:.0f}")
                        detail_cols[3].metric("Unique Opponents", int(summary["unique_opponents"]))

                    if view_mode == "Month":
                        daily_counts_df = _cast_numeric(load_player_daily_counts(selected_player, selected_month), ["games_played"])
                        if not daily_counts_df.empty:
                            st.caption("Games per day in the selected month")
                            daily_chart = daily_counts_df.set_index("game_date")[["games_played"]]
                            st.bar_chart(daily_chart)

                    games_df = _cast_numeric(
                        load_player_games(selected_player, selected_month, selected_day),
                        ["rating", "opponent_rating", "rating_diff", "score"],
                    )
                    st.dataframe(games_df, use_container_width=True)
