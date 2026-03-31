SELECT
    title,
    SUM(white_games) AS white_games,
    ROUND(SUM(white_score) * 1.0 / NULLIF(SUM(white_games), 0), 4) AS white_score_rate,
    SUM(black_games) AS black_games,
    ROUND(SUM(black_score) * 1.0 / NULLIF(SUM(black_games), 0), 4) AS black_score_rate,
    ROUND(
        (SUM(white_score) * 1.0 / NULLIF(SUM(white_games), 0))
        - (SUM(black_score) * 1.0 / NULLIF(SUM(black_games), 0)),
        4
    ) AS white_minus_black
FROM read_parquet(
    's3://{{S3_BUCKET}}/{{SILVER_PREFIX}}/player_month/year=*/month=*/title=*/bucket=*/part-000.parquet'
)
WHERE month_key BETWEEN '2026-01' AND '2026-12'
GROUP BY 1
ORDER BY white_minus_black DESC, title;
