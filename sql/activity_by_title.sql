SELECT
    title,
    total_games,
    active_players,
    avg_games_per_active_player
FROM read_parquet(
    's3://{{S3_BUCKET}}/{{GOLD_PREFIX}}/title_month_activity/year=2026/month=03/part-000.parquet'
)
ORDER BY total_games DESC;
