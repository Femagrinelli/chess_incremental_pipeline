{% macro _s3_path(path_suffix) -%}
s3://{{ env_var('S3_BUCKET') }}/{{ path_suffix }}
{%- endmacro %}

{% macro _add_roster_partitions(source_schema, snapshot_date, titles) -%}
  {% if snapshot_date %}
    {% for title in titles %}
      {% set location -%}
        {{ _s3_path(env_var('BRONZE_PREFIX', 'bronze/chess_com') ~ '/roster_daily/snapshot_date=' ~ snapshot_date ~ '/title=' ~ title ~ '/') }}
      {%- endset %}
      {% do run_query(
        "ALTER TABLE " ~ source_schema ~ ".roster_daily_external " ~
        "ADD IF NOT EXISTS PARTITION (snapshot_date='" ~ snapshot_date ~ "', title='" ~ title ~ "') " ~
        "LOCATION '" ~ location | trim ~ "'"
      ) %}
    {% endfor %}
  {% endif %}
{%- endmacro %}

{% macro _add_month_partitions(source_schema, table_name, dataset_name, month_keys, bucket_count) -%}
  {% for month_key in month_keys %}
    {% set parts = month_key.split('-') %}
    {% set year = parts[0] %}
    {% set month = parts[1] %}
    {% for bucket in range(bucket_count) %}
      {% set bucket_id = "%02d" | format(bucket) %}
      {% set location -%}
        {{ _s3_path(env_var('BRONZE_PREFIX', 'bronze/chess_com') ~ '/' ~ dataset_name ~ '/year=' ~ year ~ '/month=' ~ month ~ '/bucket=' ~ bucket_id ~ '/') }}
      {%- endset %}
      {% do run_query(
        "ALTER TABLE " ~ source_schema ~ "." ~ table_name ~ " " ~
        "ADD IF NOT EXISTS PARTITION (year='" ~ year ~ "', month='" ~ month ~ "', bucket='" ~ bucket_id ~ "') " ~
        "LOCATION '" ~ location | trim ~ "'"
      ) %}
    {% endfor %}
  {% endfor %}
{%- endmacro %}

{% macro register_bronze_external_tables() %}
  {% if not execute %}
    {{ return('') }}
  {% endif %}

  {% set source_schema = env_var('DBT_ATHENA_SOURCE_SCHEMA', 'chess_bronze_external') %}
  {% set bronze_prefix = env_var('BRONZE_PREFIX', 'bronze/chess_com') %}
  {% set month_keys = var('month_keys', []) %}
  {% set snapshot_date = var('snapshot_date', none) %}
  {% set bucket_count = env_var('PARQUET_BUCKET_COUNT', env_var('SILVER_BUCKET_COUNT', '16')) | int %}
  {% set titles = ['GM', 'WGM', 'IM', 'WIM', 'FM', 'WFM', 'NM', 'WNM', 'CM', 'WCM'] %}

  {% do run_query("CREATE DATABASE IF NOT EXISTS " ~ source_schema) %}

  {% set roster_sql %}
    CREATE EXTERNAL TABLE IF NOT EXISTS {{ source_schema }}.roster_daily_external (
      username string,
      snapshot_player_count int,
      snapshot_position int
    )
    PARTITIONED BY (
      snapshot_date string,
      title string
    )
    STORED AS PARQUET
    LOCATION '{{ _s3_path(bronze_prefix ~ "/roster_daily/") }}'
  {% endset %}
  {% do run_query(roster_sql) %}

  {% set games_core_sql %}
    CREATE EXTERNAL TABLE IF NOT EXISTS {{ source_schema }}.games_core_external (
      game_uuid string,
      game_url string,
      game_date string,
      end_time_utc string,
      time_class string,
      time_control string,
      rated boolean,
      rules string,
      opening_url string,
      opening_name string,
      initial_setup string,
      fen string,
      has_pgn boolean,
      has_tcn boolean,
      has_accuracies boolean,
      white_username string,
      white_title string,
      white_rating double,
      white_result string,
      white_score double,
      white_accuracy double,
      black_username string,
      black_title string,
      black_rating double,
      black_result string,
      black_score double,
      black_accuracy double,
      winner_color string,
      source_key_count int,
      source_player_count int,
      source_player_usernames array<string>
    )
    PARTITIONED BY (
      year string,
      month string,
      bucket string
    )
    STORED AS PARQUET
    LOCATION '{{ _s3_path(bronze_prefix ~ "/games_core/") }}'
  {% endset %}
  {% do run_query(games_core_sql) %}

  {% set player_facts_sql %}
    CREATE EXTERNAL TABLE IF NOT EXISTS {{ source_schema }}.player_game_facts_external (
      game_uuid string,
      game_url string,
      game_date string,
      end_time_utc string,
      time_class string,
      time_control string,
      rated boolean,
      rules string,
      opening_url string,
      opening_name string,
      winner_color string,
      has_pgn boolean,
      has_tcn boolean,
      has_accuracies boolean,
      username string,
      title string,
      opponent_username string,
      opponent_title string,
      color string,
      rating double,
      opponent_rating double,
      rating_diff double,
      result string,
      score double,
      accuracy double,
      opponent_accuracy double,
      is_titled_player boolean,
      is_titled_opponent boolean
    )
    PARTITIONED BY (
      year string,
      month string,
      bucket string
    )
    STORED AS PARQUET
    LOCATION '{{ _s3_path(bronze_prefix ~ "/player_game_facts/") }}'
  {% endset %}
  {% do run_query(player_facts_sql) %}

  {{ _add_roster_partitions(source_schema, snapshot_date, titles) }}
  {{ _add_month_partitions(source_schema, 'games_core_external', 'games_core', month_keys, bucket_count) }}
  {{ _add_month_partitions(source_schema, 'player_game_facts_external', 'player_game_facts', month_keys, bucket_count) }}
{% endmacro %}
