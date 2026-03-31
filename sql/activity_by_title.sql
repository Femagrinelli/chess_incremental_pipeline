SELECT
    title,
    SUM(games_played) AS total_games,
    COUNT(*) AS titled_players,
    ROUND(AVG(games_played), 2) AS avg_games_per_player
FROM read_parquet(
    's3://{{S3_BUCKET}}/{{SILVER_PREFIX}}/player_month/year=2026/month=03/title=*/bucket=*/part-000.parquet'
)
GROUP BY 1
ORDER BY total_games DESC;
