{{
  config(
    location=month_file_location(env_var('GOLD_PREFIX', 'warehouse/gold') ~ '/title_month_activity', var('month_key'))
  )
}}

with player_games as (
  select *
  from {{ ref('silver_player_game_facts') }}
  where {{ month_key_filter('year', 'month') }}
),
player_month as (
  select
    month_key,
    year,
    month,
    title,
    username,
    count(*) as games_played,
    sum(case when score = 1.0 then 1 else 0 end) as wins,
    sum(case when score = 0.5 then 1 else 0 end) as draws,
    sum(case when score = 0.0 then 1 else 0 end) as losses,
    sum(case when rated then 1 else 0 end) as rated_games,
    sum(case when not rated then 1 else 0 end) as unrated_games,
    sum(case when is_titled_opponent then 1 else 0 end) as games_vs_titled,
    sum(case when not is_titled_opponent then 1 else 0 end) as games_vs_non_titled,
    count(distinct opponent_username) as unique_opponents,
    avg(rating) as avg_rating,
    avg(score) as score_rate
  from player_games
  group by 1, 2, 3, 4, 5
)
select
  month_key,
  year,
  month,
  title,
  count(*) as active_players,
  sum(games_played) as total_games,
  sum(wins) as total_wins,
  sum(draws) as total_draws,
  sum(losses) as total_losses,
  sum(rated_games) as rated_games,
  sum(unrated_games) as unrated_games,
  sum(games_vs_titled) as games_vs_titled,
  sum(games_vs_non_titled) as games_vs_non_titled,
  round(cast(sum(games_played) as double) / nullif(count(*), 0), 2) as avg_games_per_active_player,
  round(avg(cast(unique_opponents as double)), 2) as avg_unique_opponents,
  round(avg(score_rate), 4) as avg_player_score_rate,
  round(avg(avg_rating), 2) as avg_player_rating
from player_month
group by 1, 2, 3, 4
