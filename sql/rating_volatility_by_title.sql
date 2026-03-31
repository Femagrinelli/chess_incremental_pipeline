SELECT
    month_key,
    title,
    COUNT(*) AS player_months,
    ROUND(AVG(rating_range), 2) AS avg_rating_range,
    ROUND(MEDIAN(rating_range), 2) AS median_rating_range,
    ROUND(AVG(rating_stddev), 2) AS avg_rating_stddev,
    ROUND(AVG(avg_rating), 2) AS avg_player_rating
FROM read_parquet(
    's3://{{S3_BUCKET}}/{{SILVER_PREFIX}}/player_month/year=*/month=*/title=*/bucket=*/part-000.parquet'
)
WHERE month_key BETWEEN '2026-01' AND '2026-12'
  AND games_played >= 2
  AND rating_range IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;
