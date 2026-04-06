{{
  config(
    materialized='external',
    location='s3://' ~ env_var('S3_BUCKET') ~ '/' ~ env_var('GOLD_PREFIX', 'warehouse/gold') ~ '/head_to_head_summary/part.parquet'
  )
}}

select
  pair_bucket,
  pair_key,
  player_a,
  player_b,
  sum(games_played) as games_played,
  sum(player_a_wins) as player_a_wins,
  sum(player_b_wins) as player_b_wins,
  sum(draws) as draws,
  sum(player_a_white_games) as player_a_white_games,
  sum(player_a_black_games) as player_a_black_games,
  sum(player_b_white_games) as player_b_white_games,
  sum(player_b_black_games) as player_b_black_games,
  sum(rated_games) as rated_games,
  min(first_game_date) as first_game_date,
  max(last_game_date) as last_game_date,
  round(cast(sum(player_a_wins) as double) / nullif(sum(games_played), 0), 4) as player_a_win_rate,
  round(cast(sum(player_b_wins) as double) / nullif(sum(games_played), 0), 4) as player_b_win_rate,
  round(cast(sum(draws) as double) / nullif(sum(games_played), 0), 4) as draw_rate
from {{ gold_month_glob('head_to_head_monthly') }}
group by 1, 2, 3, 4
