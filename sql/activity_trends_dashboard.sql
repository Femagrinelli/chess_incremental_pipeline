SELECT
    month_key,
    title,
    SUM(games_played) AS total_games,
    COUNT(DISTINCT username) AS active_players,
    ROUND(SUM(games_played) * 1.0 / NULLIF(COUNT(DISTINCT username), 0), 2) AS avg_games_per_active_player,
    ROUND(AVG(win_rate), 4) AS avg_player_win_rate,
    ROUND(AVG(unique_opponent_count), 2) AS avg_unique_opponents
FROM read_parquet(
    's3://{{S3_BUCKET}}/{{SILVER_PREFIX}}/player_month/year=*/month=*/title=*/bucket=*/part-000.parquet'
)
WHERE month_key BETWEEN '2026-01' AND '2026-12'
GROUP BY 1, 2
ORDER BY 1, 2;
