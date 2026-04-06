{{
  config(
    location=month_file_location(env_var('GOLD_PREFIX', 'warehouse/gold') ~ '/head_to_head_games', var('month_key'))
  )
}}

with source_games as (
  select *
  from {{ ref('silver_games_core') }}
  where {{ month_key_filter('year', 'month') }}
    and white_username is not null
    and black_username is not null
    and lower(white_username) <> lower(black_username)
),
normalized as (
  select
    month_key,
    year,
    month,
    game_uuid,
    game_url,
    game_date,
    end_time_utc,
    time_class,
    time_control,
    rated,
    rules,
    opening_url,
    opening_name,
    white_username,
    white_title,
    white_rating,
    white_result,
    white_score,
    black_username,
    black_title,
    black_rating,
    black_result,
    black_score,
    winner_color,
    case
      when lower(white_username) <= lower(black_username) then white_username
      else black_username
    end as player_a,
    case
      when lower(white_username) <= lower(black_username) then black_username
      else white_username
    end as player_b,
    case
      when lower(white_username) <= lower(black_username) then white_title
      else black_title
    end as player_a_title,
    case
      when lower(white_username) <= lower(black_username) then black_title
      else white_title
    end as player_b_title,
    case
      when lower(white_username) <= lower(black_username) then white_rating
      else black_rating
    end as player_a_rating,
    case
      when lower(white_username) <= lower(black_username) then black_rating
      else white_rating
    end as player_b_rating,
    case
      when lower(white_username) <= lower(black_username) then 'white'
      else 'black'
    end as player_a_color,
    case
      when lower(white_username) <= lower(black_username) then 'black'
      else 'white'
    end as player_b_color,
    case
      when lower(white_username) <= lower(black_username) then white_score
      else black_score
    end as player_a_score,
    case
      when lower(white_username) <= lower(black_username) then black_score
      else white_score
    end as player_b_score
  from source_games
),
final as (
  select
    month_key,
    year,
    month,
    substr(md5(concat(player_a, '::', player_b)), 1, 2) as pair_bucket,
    concat(player_a, '::', player_b) as pair_key,
    player_a,
    player_b,
    player_a_title,
    player_b_title,
    player_a_color,
    player_b_color,
    player_a_rating,
    player_b_rating,
    player_a_score,
    player_b_score,
    case
      when player_a_score = player_b_score then 'draw'
      when player_a_score > player_b_score then player_a
      else player_b
    end as winner_username,
    case
      when player_a_score = player_b_score then 'draw'
      when player_a_score > player_b_score then player_a
      else player_b
    end as result_label,
    game_uuid,
    game_url,
    game_date,
    end_time_utc,
    time_class,
    time_control,
    rated,
    rules,
    opening_url,
    opening_name,
    white_username,
    black_username,
    winner_color
  from normalized
)
select *
from final
