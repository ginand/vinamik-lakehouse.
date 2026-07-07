{{
  config(
    materialized = 'external',
    location     = gold_path('dq_monitoring_gold'),
    format       = 'parquet'
  )
}}

-- ─────────────────────────────────────────────────────────
-- Gold KPI: Data Quality Monitoring Dashboard
-- Source: quarantine tables trên ADLS Gen2
-- Dùng cho Power BI "Data Quality Monitor" page
-- ─────────────────────────────────────────────────────────

{% set quarantine_tables = [
    ('transactions_silver',    'transactions'),
    ('general_ledger_silver',  'general_ledger'),
    ('ar_silver',              'accounts_receivable'),
    ('ap_silver',              'accounts_payable'),
] %}

{% for silver_name, display_name in quarantine_tables %}

{% if not loop.first %}UNION ALL{% endif %}

SELECT
    '{{ display_name }}'                                  AS source_table,
    _error_type                                           AS error_type,
    _error_column                                         AS error_column,
    CAST(_quarantined_at AS DATE)                         AS quarantine_date,
    CAST(_quarantined_at AS VARCHAR)                      AS quarantine_timestamp,
    COUNT(*)                                              AS error_count
FROM delta_scan(
    'abfss://quarantine@{{ var("storage_account") }}.dfs.core.windows.net/{{ silver_name }}'
)
GROUP BY
    _error_type,
    _error_column,
    CAST(_quarantined_at AS DATE),
    CAST(_quarantined_at AS VARCHAR)

{% endfor %}
