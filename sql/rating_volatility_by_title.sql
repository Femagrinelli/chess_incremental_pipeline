SELECT
    month_key,
    title,
    player_months,
    avg_games_per_player,
    avg_rating_range,
    median_rating_range,
    avg_rating_stddev,
    avg_player_rating
FROM read_parquet(
    's3://{{S3_BUCKET}}/{{GOLD_PREFIX}}/title_month_rating_volatility/year=*/month=*/part-000.parquet'
)
WHERE month_key BETWEEN '2026-01' AND '2026-12'
ORDER BY 1, 2;
