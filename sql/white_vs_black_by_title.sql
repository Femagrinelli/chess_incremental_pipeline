SELECT
    title,
    color,
    COUNT(*) AS games,
    ROUND(AVG(score), 4) AS avg_score
FROM read_parquet(
    's3://{{S3_BUCKET}}/{{SILVER_PREFIX}}/player_games/year=2026/month=03/title=*/bucket=*/part-000.parquet'
)
GROUP BY 1, 2
ORDER BY 1, 2;
