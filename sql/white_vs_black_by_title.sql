SELECT
    title,
    white_score_rate,
    black_score_rate,
    white_minus_black,
    total_games
FROM read_parquet(
    's3://{{S3_BUCKET}}/{{GOLD_PREFIX}}/title_month_color_performance/year=2026/month=03/part-000.parquet'
)
ORDER BY white_minus_black DESC, title;
