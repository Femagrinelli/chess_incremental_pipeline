{{ config(materialized='ephemeral') }}

select
  cast(snapshot_date as varchar) as snapshot_date,
  cast(title as varchar) as title,
  cast(username as varchar) as username,
  cast(snapshot_player_count as integer) as snapshot_player_count,
  cast(snapshot_position as integer) as snapshot_position
from read_parquet(
  's3://{{ env_var("S3_BUCKET") }}/{{ env_var("BRONZE_PREFIX", "warehouse/bronze") }}/roster_daily/snapshot_date=*/title=*/part-000.parquet',
  hive_partitioning=true
)
