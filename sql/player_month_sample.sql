SELECT
    year,
    month,
    title,
    username,
    games_played,
    wins,
    draws,
    losses,
    win_rate,
    white_score_rate,
    black_score_rate
FROM read_parquet(
    's3://{{S3_BUCKET}}/{{SILVER_PREFIX}}/player_month/year=2026/month=03/title=GM/bucket=*/part-000.parquet'
)
ORDER BY games_played DESC
LIMIT 20;
