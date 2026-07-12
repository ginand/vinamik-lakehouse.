{{
  config(
    materialized = 'external',
    location     = gold_path('budget_vs_actual_gold'),
    format       = 'parquet'
  )
}}

-- ─────────────────────────────────────────────────────────
-- Gold KPI 4: Ngân sách vs Thực tế (Budget vs Actual)
-- Source: budget_plan_silver + general_ledger_silver + transactions_silver
-- ─────────────────────────────────────────────────────────

WITH budget AS (
    SELECT
        fiscal_year,
        month AS budget_month,
        cost_center,
        SUM(budget_amount) AS budget_total
    FROM {{ silver_source('budget_plan_silver') }}
    WHERE cost_center IS NOT NULL
      AND fiscal_year IS NOT NULL
      AND month IS NOT NULL
    GROUP BY 
        fiscal_year,
        month,
        cost_center
),

actual AS (
    SELECT
        txn.fiscal_year,
        txn.fiscal_period AS actual_month,
        gl.cost_center,
        SUM(coalesce(gl.amount_vnd, gl.amount)) AS actual_total
    FROM {{ silver_source('general_ledger_silver') }} gl
    INNER JOIN {{ silver_source('transactions_silver') }} txn
           ON gl.txn_id = txn.txn_id
    WHERE gl._is_deleted = FALSE
      AND gl.dq_is_clean = TRUE
      AND gl.debit_credit = 'D' -- Chi phí thường ghi nợ
      AND gl.account_id LIKE '6%' -- Các tài khoản chi phí
      AND gl.cost_center IS NOT NULL
      AND txn.status = 'POSTED'
    GROUP BY 
        txn.fiscal_year,
        txn.fiscal_period,
        gl.cost_center
)

SELECT
    COALESCE(b.cost_center, a.cost_center) AS cost_center,
    COALESCE(b.fiscal_year, a.fiscal_year) AS fiscal_year,
    COALESCE(b.budget_month, a.actual_month) AS fiscal_period,
    COALESCE(b.budget_total, 0) AS budget,
    COALESCE(a.actual_total, 0) AS actual,
    COALESCE(a.actual_total, 0) - COALESCE(b.budget_total, 0) AS variance_vnd,
    CASE
        WHEN COALESCE(a.actual_total, 0) > COALESCE(b.budget_total, 0) THEN 'OVER_BUDGET'
        WHEN COALESCE(a.actual_total, 0) < COALESCE(b.budget_total, 0) THEN 'UNDER_BUDGET'
        ELSE 'ON_TRACK'
    END AS status_flag,
    CASE
        WHEN COALESCE(b.budget_total, 0) = 0 THEN NULL
        ELSE ROUND((COALESCE(a.actual_total, 0) / b.budget_total) * 100, 2)
    END AS completion_pct,
    NOW() AS _gold_computed_at
FROM budget b
FULL OUTER JOIN actual a
    ON b.fiscal_year = a.fiscal_year
   AND b.budget_month = a.actual_month
   AND b.cost_center = a.cost_center
