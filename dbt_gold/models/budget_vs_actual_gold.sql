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
        fiscal_period AS budget_month,
        cost_center,
        SUM(budgeted_amount_vnd) AS budget_amount
    FROM {{ silver_source('budget_plan_silver') }}
    WHERE cost_center IS NOT NULL
    GROUP BY 
        fiscal_period,
        cost_center
),

actual AS (
    SELECT
        txn.fiscal_period AS actual_month,
        gl.cost_center,
        SUM(coalesce(gl.amount_vnd, gl.amount)) AS actual_amount
    FROM {{ silver_source('general_ledger_silver') }} gl
    LEFT JOIN {{ silver_source('transactions_silver') }} txn
           ON gl.txn_id = txn.txn_id
    WHERE gl._is_deleted = FALSE
      AND gl.dq_is_clean = TRUE
      AND gl.debit_credit = 'D' -- Chi phí thường ghi nợ
      AND gl.account_id LIKE '6%' -- Các tài khoản chi phí
      AND gl.cost_center IS NOT NULL
      AND txn.status = 'POSTED'
    GROUP BY 
        txn.fiscal_period,
        gl.cost_center
)

SELECT
    COALESCE(b.budget_month, a.actual_month) AS Month,
    COALESCE(b.cost_center, a.cost_center) AS Cost_Center,
    COALESCE(b.budget_amount, 0) AS Budget,
    COALESCE(a.actual_amount, 0) AS Actual,
    COALESCE(a.actual_amount, 0) - COALESCE(b.budget_amount, 0) AS Variance,
    CASE
        WHEN COALESCE(b.budget_amount, 0) = 0 THEN NULL
        ELSE ROUND((COALESCE(a.actual_amount, 0) / b.budget_amount) * 100, 2)
    END AS Completion,
    NOW() AS _gold_computed_at
FROM budget b
FULL OUTER JOIN actual a
    ON b.budget_month = a.actual_month
   AND b.cost_center = a.cost_center
