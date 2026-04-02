select
  cast(snapshot_date as varchar) as snapshot_date,
  cast(title as varchar) as title,
  cast(username as varchar) as username,
  cast(snapshot_player_count as integer) as snapshot_player_count,
  cast(snapshot_position as integer) as snapshot_position
from {{ source('bronze_external', 'roster_daily_external') }}
