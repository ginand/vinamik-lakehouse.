{{
  config(
    materialized = 'external',
    location     = gold_path('budget_vs_actual_gold'),
    format       = 'delta'
  )
}}

-- ─────────────────────────────────────────────────────────
-- Gold KPI 4: Ngân sách vs Thực tế (Budget vs Actual)
-- Source: budget_plan_silver + general_ledger_silver + transactions_silver
-- ─────────────────────────────────────────────────────────

WITH budget AS (
    SELECT
        JSON_EXTRACT_STRING(raw_payload, '$.cost_center')         AS cost_center,
        JSON_EXTRACT_STRING(raw_payload, '$.gl_account')          AS gl_account,
        CAST(JSON_EXTRACT(raw_payload, '$.fiscal_year')   AS INT) AS fiscal_year,
        CAST(JSON_EXTRACT(raw_payload, '$.fiscal_period') AS INT) AS fiscal_period,
        CAST(JSON_EXTRACT(raw_payload, '$.budgeted_amount_vnd') AS DOUBLE) AS budgeted_amount_vnd
    FROM {{ silver_source('budget_plan_silver') }}
    WHERE _is_deleted = FALSE
      AND JSON_EXTRACT_STRING(raw_payload, '$.cost_center') IS NOT NULL
),

budget_agg AS (
    SELECT
        cost_center, gl_account, fiscal_year, fiscal_period,
        ROUND(SUM(budgeted_amount_vnd), 0) AS budgeted_amount_vnd
    FROM budget
    GROUP BY cost_center, gl_account, fiscal_year, fiscal_period
),

actual AS (
    SELECT
        gl.cost_center,
        gl.account_id                          AS gl_account,
        txn.fiscal_year,
        txn.fiscal_period,
        ROUND(SUM(gl.amount_vnd), 0)           AS actual_amount_vnd
    FROM {{ silver_source('general_ledger_silver') }} gl
    LEFT JOIN {{ silver_source('transactions_silver') }} txn
           ON gl.txn_id = txn.txn_id
    WHERE gl._is_deleted = FALSE
      AND gl.dq_is_clean = TRUE
      AND gl.debit_credit = 'D'
      AND REGEXP_MATCHES(gl.account_id, '^[67]')
      AND txn.status = 'POSTED'
    GROUP BY gl.cost_center, gl.account_id, txn.fiscal_year, txn.fiscal_period
)

SELECT
    COALESCE(b.cost_center, a.cost_center)         AS cost_center,
    COALESCE(b.gl_account,  a.gl_account)          AS gl_account,
    COALESCE(b.fiscal_year, a.fiscal_year)         AS fiscal_year,
    COALESCE(b.fiscal_period, a.fiscal_period)     AS fiscal_period,
    COALESCE(b.budgeted_amount_vnd, 0)             AS budgeted_amount_vnd,
    COALESCE(a.actual_amount_vnd, 0)               AS actual_amount_vnd,
    COALESCE(a.actual_amount_vnd, 0) - COALESCE(b.budgeted_amount_vnd, 0) AS variance_vnd,
    CASE
        WHEN COALESCE(b.budgeted_amount_vnd, 0) = 0 THEN NULL
        ELSE ROUND(COALESCE(a.actual_amount_vnd, 0) / b.budgeted_amount_vnd * 100, 2)
    END                                            AS achievement_pct,
    CASE
        WHEN COALESCE(a.actual_amount_vnd, 0) > COALESCE(b.budgeted_amount_vnd, 0) * 1.1 THEN 'OVER_BUDGET'
        WHEN COALESCE(a.actual_amount_vnd, 0) < COALESCE(b.budgeted_amount_vnd, 0) * 0.9 THEN 'UNDER_BUDGET'
        ELSE 'ON_TRACK'
    END                                            AS status_flag,
    NOW()                                          AS _gold_computed_at
FROM budget_agg b
FULL OUTER JOIN actual a
    ON b.cost_center  = a.cost_center
   AND b.gl_account   = a.gl_account
   AND b.fiscal_year  = a.fiscal_year
   AND b.fiscal_period = a.fiscal_period
