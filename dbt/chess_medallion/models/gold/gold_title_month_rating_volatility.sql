{{
  config(
    location=month_file_location(env_var('GOLD_PREFIX', 'warehouse/gold') ~ '/title_month_rating_volatility', var('month_key'))
  )
}}

with player_games as (
  select *
  from {{ ref('silver_player_game_facts') }}
  where {{ month_key_filter('year', 'month') }}
    and rating is not null
),
player_month as (
  select
    month_key,
    year,
    month,
    title,
    username,
    count(*) as games_played,
    avg(rating) as avg_rating,
    min(rating) as min_rating,
    max(rating) as max_rating,
    max(rating) - min(rating) as rating_range,
    stddev_pop(rating) as rating_stddev
  from player_games
  group by 1, 2, 3, 4, 5
)
select
  month_key,
  year,
  month,
  title,
  count(*) as player_months,
  round(avg(cast(games_played as double)), 2) as avg_games_per_player,
  round(avg(rating_range), 2) as avg_rating_range,
  round(median(rating_range), 2) as median_rating_range,
  round(min(rating_range), 2) as min_rating_range,
  round(max(rating_range), 2) as max_rating_range,
  round(avg(rating_stddev), 2) as avg_rating_stddev,
  round(avg(avg_rating), 2) as avg_player_rating
from player_month
where games_played >= 2
  and rating_range is not null
group by 1, 2, 3, 4
