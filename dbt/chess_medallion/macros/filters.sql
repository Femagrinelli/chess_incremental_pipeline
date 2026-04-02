{% macro month_key_expr(year_col, month_col) -%}
concat(cast({{ year_col }} as varchar), '-', lpad(cast({{ month_col }} as varchar), 2, '0'))
{%- endmacro %}

{% macro month_key_filter(year_col, month_col) -%}
  {% set month_keys = var('month_keys', []) %}
  {% if month_keys and month_keys | length > 0 %}
    {{ month_key_expr(year_col, month_col) }} in (
      {% for month_key in month_keys -%}
        '{{ month_key }}'{% if not loop.last %}, {% endif %}
      {%- endfor %}
    )
  {% else %}
    1 = 1
  {% endif %}
{%- endmacro %}

{% macro snapshot_date_filter(column_name) -%}
  {% set snapshot_date = var('snapshot_date', none) %}
  {% if snapshot_date %}
    {{ column_name }} = '{{ snapshot_date }}'
  {% else %}
    1 = 1
  {% endif %}
{%- endmacro %}
