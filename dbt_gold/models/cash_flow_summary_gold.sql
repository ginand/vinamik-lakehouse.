{{
  config(
    materialized = 'external',
    location     = gold_path('cash_flow_summary_gold'),
    format       = 'delta'
  )
}}

-- ─────────────────────────────────────────────────────────
-- Gold KPI 6: Tóm tắt dòng tiền (Cash Flow Summary)
-- Source: transactions_silver
-- ─────────────────────────────────────────────────────────

SELECT
    company_code,
    fiscal_year,
    fiscal_period,
    posting_month,
    CASE
        WHEN doc_type IN ('DZ')         THEN 'INFLOW_AR_COLLECTION'
        WHEN doc_type IN ('KZ')         THEN 'OUTFLOW_AP_PAYMENT'
        WHEN doc_type IN ('RV', 'DR')   THEN 'REVENUE_INVOICE'
        WHEN doc_type IN ('KR', 'RE')   THEN 'PURCHASE_INVOICE'
        WHEN doc_type IN ('SA')         THEN 'GENERAL_POSTING'
        WHEN doc_type IN ('WA')         THEN 'GOODS_MOVEMENT'
        ELSE 'OTHER'
    END                                    AS flow_category,
    COUNT(txn_id)                          AS num_transactions,
    ROUND(SUM(total_debit), 0)             AS total_debit_vnd,
    ROUND(SUM(total_credit), 0)            AS total_credit_vnd,
    ROUND(SUM(total_credit) - SUM(total_debit), 0) AS net_flow_vnd,
    NOW()                                  AS _gold_computed_at
FROM {{ silver_source('transactions_silver') }}
WHERE _is_deleted = FALSE
  AND status = 'POSTED'
GROUP BY
    company_code, fiscal_year, fiscal_period, posting_month,
    CASE
        WHEN doc_type IN ('DZ')         THEN 'INFLOW_AR_COLLECTION'
        WHEN doc_type IN ('KZ')         THEN 'OUTFLOW_AP_PAYMENT'
        WHEN doc_type IN ('RV', 'DR')   THEN 'REVENUE_INVOICE'
        WHEN doc_type IN ('KR', 'RE')   THEN 'PURCHASE_INVOICE'
        WHEN doc_type IN ('SA')         THEN 'GENERAL_POSTING'
        WHEN doc_type IN ('WA')         THEN 'GOODS_MOVEMENT'
        ELSE 'OTHER'
    END
