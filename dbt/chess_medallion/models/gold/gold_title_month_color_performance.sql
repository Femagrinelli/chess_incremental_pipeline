{{
  config(
    external_location='s3://' ~ env_var('S3_BUCKET') ~ '/' ~ env_var('GOLD_PREFIX', 'gold/chess_com') ~ '/title_month_color_performance/',
    partitioned_by=['year', 'month']
  )
}}

with player_games as (
  select *
  from {{ ref('silver_player_game_facts') }}
  where {{ month_key_filter('year', 'month') }}
)
select
  month_key,
  year,
  month,
  title,
  sum(case when color = 'white' then 1 else 0 end) as white_games,
  round(sum(case when color = 'white' then score else 0 end), 2) as white_score_points,
  round(
    cast(sum(case when color = 'white' then score else 0 end) as double)
    / nullif(sum(case when color = 'white' then 1 else 0 end), 0),
    4
  ) as white_score_rate,
  sum(case when color = 'black' then 1 else 0 end) as black_games,
  round(sum(case when color = 'black' then score else 0 end), 2) as black_score_points,
  round(
    cast(sum(case when color = 'black' then score else 0 end) as double)
    / nullif(sum(case when color = 'black' then 1 else 0 end), 0),
    4
  ) as black_score_rate,
  round(
    (
      cast(sum(case when color = 'white' then score else 0 end) as double)
      / nullif(sum(case when color = 'white' then 1 else 0 end), 0)
    ) - (
      cast(sum(case when color = 'black' then score else 0 end) as double)
      / nullif(sum(case when color = 'black' then 1 else 0 end), 0)
    ),
    4
  ) as white_minus_black,
  count(*) as total_games
from player_games
group by 1, 2, 3, 4
