{{
  config(
    materialized = 'external',
    location     = gold_path('ap_aging_gold'),
    format       = 'parquet'
  )
}}

-- ─────────────────────────────────────────────────────────
-- Gold KPI 3: Phân tích công nợ phải trả (AP Aging)
-- Source: ap_silver + vendors_silver
-- ─────────────────────────────────────────────────────────

WITH ap AS (
    SELECT *
    FROM {{ silver_source('ap_silver') }}
    WHERE _is_deleted = FALSE
      AND status NOT IN ('PAID')
),

vendors AS (
    SELECT vendor_id, vendor_name, vendor_type, country
    FROM {{ silver_source('vendors_silver') }}
    WHERE _is_deleted = FALSE
)

SELECT
    ap.vendor_id,
    v.vendor_name,
    v.vendor_type,
    v.country,
    ap.aging_bucket,
    ap.invoice_month,
    COUNT(ap.ap_id)                                   AS num_invoices,
    ROUND(SUM(ap.outstanding_vnd), 0)                 AS total_outstanding_vnd,
    ROUND(SUM(coalesce(ap.amount_vnd, ap.amount)), 0)                      AS total_invoiced_vnd,
    ROUND(SUM(ap.paid_amount), 0)                     AS total_paid_vnd,
    ROUND(AVG(ap.overdue_days), 1)                    AS avg_overdue_days,
    ROUND(MAX(ap.overdue_days), 0)                    AS max_overdue_days,
    ROUND(SUM(ap.paid_amount) / NULLIF(SUM(coalesce(ap.amount_vnd, ap.amount)), 0) * 100, 2) AS payment_rate_pct,
    NOW()                                             AS _gold_computed_at
FROM ap
LEFT JOIN vendors v ON ap.vendor_id = v.vendor_id
GROUP BY
    ap.vendor_id, v.vendor_name, v.vendor_type,
    v.country, ap.aging_bucket, ap.invoice_month
