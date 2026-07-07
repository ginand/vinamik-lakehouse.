{{
  config(
    materialized = 'external',
    location     = gold_path('ar_aging_gold'),
    format       = 'parquet'
  )
}}

-- ─────────────────────────────────────────────────────────
-- Gold KPI 2: Phân tích công nợ phải thu (AR Aging)
-- Source: ar_silver + customers_silver
-- ─────────────────────────────────────────────────────────

WITH ar AS (
    SELECT *
    FROM {{ silver_source('ar_silver') }}
    WHERE _is_deleted = FALSE
      AND status NOT IN ('PAID')
),

customers AS (
    SELECT customer_id, customer_name, customer_type,
           province, sales_region, credit_limit
    FROM {{ silver_source('customers_silver') }}
    WHERE _is_deleted = FALSE
)

SELECT
    ar.customer_id,
    c.customer_name,
    c.customer_type,
    ar.sales_channel,
    c.sales_region,
    c.province,
    ar.aging_bucket,
    ar.invoice_month,
    COUNT(ar.ar_id)                                   AS num_invoices,
    ROUND(SUM(ar.outstanding_vnd), 0)                 AS total_outstanding_vnd,
    ROUND(SUM(ar.amount_vnd), 0)                      AS total_invoiced_vnd,
    ROUND(SUM(ar.paid_amount), 0)                     AS total_paid_vnd,
    ROUND(AVG(ar.overdue_days), 1)                    AS avg_overdue_days,
    ROUND(MAX(ar.overdue_days), 0)                    AS max_overdue_days,
    ROUND(MAX(c.credit_limit), 0)                     AS credit_limit,
    ROUND(SUM(ar.paid_amount) / NULLIF(SUM(ar.amount_vnd), 0) * 100, 2) AS collection_rate_pct,
    NOW()                                             AS _gold_computed_at
FROM ar
LEFT JOIN customers c ON ar.customer_id = c.customer_id
GROUP BY
    ar.customer_id, c.customer_name, c.customer_type,
    ar.sales_channel, c.sales_region, c.province,
    ar.aging_bucket, ar.invoice_month
