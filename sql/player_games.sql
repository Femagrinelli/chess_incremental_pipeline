select
  count(*)
from read_parquet(
  's3://chess-games-lake/bronze/chess_com/games_core/year=2025/month=12/*/*.parquet',
  hive_partitioning=true
)
limit 50;