{{
  config(
    external_location='s3://' ~ env_var('S3_BUCKET') ~ '/' ~ env_var('SILVER_PREFIX', 'silver/chess_com') ~ '/roster_daily/',
    partitioned_by=['snapshot_date', 'title']
  )
}}

with source_rows as (
  select *
  from {{ ref('bronze_roster_daily') }}
  where {{ snapshot_date_filter('snapshot_date') }}
),
ranked as (
  select
    *,
    row_number() over (
      partition by snapshot_date, title, username
      order by snapshot_position asc, username asc
    ) as rn
  from source_rows
)
select
  snapshot_date,
  title,
  username,
  snapshot_player_count,
  snapshot_position,
  row_number() over (
    partition by snapshot_date, title
    order by snapshot_position asc, username asc
  ) as snapshot_rank
from ranked
where rn = 1
