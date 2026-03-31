SELECT
    month_key,
    title,
    total_games,
    active_players,
    avg_games_per_active_player,
    avg_player_win_rate,
    avg_unique_opponents
FROM read_parquet(
    's3://{{S3_BUCKET}}/{{GOLD_PREFIX}}/title_month_activity/year=*/month=*/part-000.parquet'
)
WHERE month_key BETWEEN '2026-01' AND '2026-12'
ORDER BY 1, 2;
