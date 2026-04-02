{{
  config(
    external_location='s3://' ~ env_var('S3_BUCKET') ~ '/' ~ env_var('SILVER_PREFIX', 'silver/chess_com') ~ '/player_game_facts/',
    partitioned_by=['year', 'month']
  )
}}

with source_rows as (
  select *
  from {{ ref('bronze_player_game_facts') }}
  where {{ month_key_filter('year', 'month') }}
),
ranked as (
  select
    *,
    row_number() over (
      partition by year, month, game_uuid, username
      order by
        case when title is not null then 0 else 1 end asc,
        case when opponent_title is not null then 0 else 1 end asc,
        color asc
    ) as rn
  from source_rows
  where game_uuid is not null
    and username is not null
)
select
  month_key,
  year,
  month,
  game_uuid,
  game_url,
  cast(game_date as date) as game_date,
  end_time_utc,
  lower(time_class) as time_class,
  time_control,
  coalesce(rated, false) as rated,
  rules,
  opening_url,
  opening_name,
  winner_color,
  coalesce(has_pgn, false) as has_pgn,
  coalesce(has_tcn, false) as has_tcn,
  coalesce(has_accuracies, false) as has_accuracies,
  username,
  title,
  opponent_username,
  opponent_title,
  color,
  rating,
  opponent_rating,
  rating_diff,
  result,
  score,
  accuracy,
  opponent_accuracy,
  coalesce(is_titled_player, false) as is_titled_player,
  coalesce(is_titled_opponent, false) as is_titled_opponent,
  case
    when score = 1.0 then 'win'
    when score = 0.5 then 'draw'
    when score = 0.0 then 'loss'
    else 'unknown'
  end as score_band
from ranked
where rn = 1
  and coalesce(is_titled_player, false)
  and title is not null
