{% macro month_file_location(prefix, month_key) -%}
  {% set resolved_month_key = month_key if month_key is not none else '1970-01' %}
  {% set parts = resolved_month_key.split('-') %}
  {% set year = parts[0] %}
  {% set month = parts[1] %}
  {{ return("s3://" ~ env_var("S3_BUCKET") ~ "/" ~ prefix ~ "/year=" ~ year ~ "/month=" ~ month ~ "/part.parquet") }}
{%- endmacro %}

{% macro snapshot_file_location(prefix, snapshot_date) -%}
  {% set resolved_snapshot_date = snapshot_date if snapshot_date is not none else '1970-01-01' %}
  {{ return("s3://" ~ env_var("S3_BUCKET") ~ "/" ~ prefix ~ "/snapshot_date=" ~ resolved_snapshot_date ~ "/part.parquet") }}
{%- endmacro %}

{% macro gold_month_glob(dataset_name) -%}
  read_parquet('s3://{{ env_var("S3_BUCKET") }}/{{ env_var("GOLD_PREFIX", "warehouse/gold") }}/{{ dataset_name }}/year=*/month=*/part.parquet', hive_partitioning=true)
{%- endmacro %}
