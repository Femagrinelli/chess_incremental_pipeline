{{
  config(
    location=month_file_location(env_var('GOLD_PREFIX', 'warehouse/gold') ~ '/head_to_head_monthly', var('month_key'))
  )
}}

with pair_games as (
  select *
  from {{ ref('gold_head_to_head_games') }}
  where {{ month_key_filter('year', 'month') }}
)
select
  month_key,
  year,
  month,
  pair_bucket,
  pair_key,
  player_a,
  player_b,
  count(*) as games_played,
  sum(case when player_a_score = 1.0 then 1 else 0 end) as player_a_wins,
  sum(case when player_b_score = 1.0 then 1 else 0 end) as player_b_wins,
  sum(case when player_a_score = 0.5 and player_b_score = 0.5 then 1 else 0 end) as draws,
  sum(case when player_a_color = 'white' then 1 else 0 end) as player_a_white_games,
  sum(case when player_a_color = 'black' then 1 else 0 end) as player_a_black_games,
  sum(case when player_b_color = 'white' then 1 else 0 end) as player_b_white_games,
  sum(case when player_b_color = 'black' then 1 else 0 end) as player_b_black_games,
  sum(case when rated then 1 else 0 end) as rated_games,
  min(game_date) as first_game_date,
  max(game_date) as last_game_date
from pair_games
group by 1, 2, 3, 4, 5, 6, 7
