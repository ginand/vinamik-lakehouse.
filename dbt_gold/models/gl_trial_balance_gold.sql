{{
  config(
    materialized = 'external',
    location     = gold_path('gl_trial_balance_gold'),
    format       = 'parquet'
  )
}}

-- ─────────────────────────────────────────────────────────
-- Gold KPI 5: Bảng cân đối số phát sinh (GL Trial Balance)
-- Source: general_ledger_silver + transactions_silver
-- ─────────────────────────────────────────────────────────

WITH gl AS (
    SELECT *
    FROM {{ silver_source('general_ledger_silver') }}
    WHERE _is_deleted = FALSE
      AND dq_is_clean = TRUE
),

txn AS (
    SELECT txn_id, company_code, fiscal_year, fiscal_period, posting_month, status
    FROM {{ silver_source('transactions_silver') }}
    WHERE _is_deleted = FALSE
      AND status = 'POSTED'
)

SELECT
    gl.account_id,
    txn.company_code,
    txn.fiscal_year,
    txn.fiscal_period,
    txn.posting_month,
    ROUND(SUM(CASE WHEN gl.debit_credit = 'D' THEN gl.amount_vnd ELSE 0 END), 0) AS total_debit_vnd,
    ROUND(SUM(CASE WHEN gl.debit_credit = 'C' THEN gl.amount_vnd ELSE 0 END), 0) AS total_credit_vnd,
    COUNT(gl.gl_id)                                                                AS num_line_items,
    ROUND(
        SUM(CASE WHEN gl.debit_credit = 'D' THEN gl.amount_vnd ELSE 0 END) -
        SUM(CASE WHEN gl.debit_credit = 'C' THEN gl.amount_vnd ELSE 0 END),
        0
    )                                                                              AS net_balance_vnd,
    NOW()                                                                          AS _gold_computed_at
FROM gl
LEFT JOIN txn ON gl.txn_id = txn.txn_id
GROUP BY
    gl.account_id, txn.company_code,
    txn.fiscal_year, txn.fiscal_period, txn.posting_month
