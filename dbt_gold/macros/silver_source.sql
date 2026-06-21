-- ─────────────────────────────────────────────────────────
-- macros/silver_source.sql
-- Helper macro: trả về đường dẫn đọc Delta table từ Silver trên ADLS Gen2
-- Sử dụng: {{ silver_source('transactions_silver') }}
-- ─────────────────────────────────────────────────────────

{% macro silver_source(table_name) %}
  delta_scan(
    'abfss://{{ var("silver_container") }}@{{ var("storage_account") }}.dfs.core.windows.net/{{ table_name }}'
  )
{% endmacro %}

{% macro gold_path(table_name) %}
  'abfss://{{ var("gold_container") }}@{{ var("storage_account") }}.dfs.core.windows.net/{{ table_name }}'
{% endmacro %}
