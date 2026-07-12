{{
  config(
    materialized = 'external',
    location     = gold_path('revenue_by_product_gold'),
    format       = 'parquet'
  )
}}

-- ─────────────────────────────────────────────────────────
-- Gold KPI 1: Doanh thu theo dòng sản phẩm VinaMilk
-- Source: general_ledger_silver + transactions_silver
-- ─────────────────────────────────────────────────────────

WITH gl AS (
    SELECT *
    FROM {{ silver_source('general_ledger_silver') }}
    WHERE _is_deleted = FALSE
      AND dq_is_clean = TRUE
      AND account_id LIKE '511%'
      AND debit_credit = 'C'
      AND coalesce(amount_vnd, amount) > 0
),

txn AS (
    SELECT txn_id, company_code, fiscal_year, fiscal_period,
           posting_date, posting_month, currency, status
    FROM {{ silver_source('transactions_silver') }}
    WHERE _is_deleted = FALSE
      AND status = 'POSTED'
),

joined AS (
    SELECT
        gl.gl_id,
        gl.txn_id,
        gl.account_id,
        coalesce(gl.amount_vnd, gl.amount) AS amount_vnd,
        gl.cost_center,
        SUBSTR(gl.account_id, 1, 4) AS product_line,
        txn.company_code,
        txn.fiscal_year,
        txn.fiscal_period,
        txn.posting_month
    FROM gl
    INNER JOIN txn ON gl.txn_id = txn.txn_id
)

SELECT
    product_line,
    CASE product_line
        WHEN '5111' THEN 'Sữa tươi UHT'
        WHEN '5112' THEN 'Sữa đặc Ông Thọ / Ngôi Sao'
        WHEN '5113' THEN 'Sữa bột Dielac'
        WHEN '5114' THEN 'Sữa chua / ProYogurt'
        WHEN '5115' THEN 'Kem & Nước trái cây Vfresh'
        ELSE 'Khác'
    END                                   AS product_name,
    company_code,
    cost_center,
    fiscal_year,
    fiscal_period,
    posting_month,
    ROUND(SUM(amount_vnd), 0)             AS revenue_vnd,
    COUNT(gl_id)                          AS num_line_items,
    COUNT(DISTINCT txn_id)                AS num_transactions,
    ROUND(AVG(amount_vnd), 0)             AS avg_revenue_per_line,
    NOW()                                 AS _gold_computed_at
FROM joined
GROUP BY
    product_line, company_code, cost_center,
    fiscal_year, fiscal_period, posting_month
