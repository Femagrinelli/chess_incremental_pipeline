{{
  config(
    external_location='s3://' ~ env_var('S3_BUCKET') ~ '/' ~ env_var('SILVER_PREFIX', 'silver/chess_com') ~ '/games_core/',
    partitioned_by=['year', 'month']
  )
}}

with source_rows as (
  select *
  from {{ ref('bronze_games_core') }}
  where {{ month_key_filter('year', 'month') }}
),
ranked as (
  select
    *,
    row_number() over (
      partition by year, month, game_uuid
      order by source_key_count desc, source_player_count desc, game_url asc
    ) as rn
  from source_rows
  where game_uuid is not null
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
  initial_setup,
  fen,
  coalesce(has_pgn, false) as has_pgn,
  coalesce(has_tcn, false) as has_tcn,
  coalesce(has_accuracies, false) as has_accuracies,
  white_username,
  white_title,
  white_rating,
  white_result,
  white_score,
  white_accuracy,
  black_username,
  black_title,
  black_rating,
  black_result,
  black_score,
  black_accuracy,
  winner_color,
  source_key_count,
  source_player_count,
  source_player_usernames,
  case
    when white_title is not null and black_title is not null then true
    else false
  end as is_titled_matchup
from ranked
where rn = 1
