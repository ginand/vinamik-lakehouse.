-- ============================================================
-- VinaMilk Data Lakehouse — PostgreSQL Schema Initialization
-- Mock SAP S/4HANA ERP Database (VAS - Vietnamese Accounting Standards)
-- ============================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ─────────────────────────────────────────────────────────
-- TABLE 1: company_codes — Công ty (SAP Company Code / BUKRS)
-- VinaMilk group entities
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS company_codes (
    bukrs           VARCHAR(4)   PRIMARY KEY,
    company_name    VARCHAR(200) NOT NULL,
    country         CHAR(2)      DEFAULT 'VN',
    currency        CHAR(3)      DEFAULT 'VND',
    tax_code        VARCHAR(20),
    address         VARCHAR(300),
    fiscal_year_variant VARCHAR(2) DEFAULT 'V1',  -- V1 = Jan-Dec
    is_active       BOOLEAN      DEFAULT TRUE,
    created_at      TIMESTAMP    DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────
-- TABLE 2: plants — Nhà máy / Chi nhánh (SAP Plant / WERKS)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS plants (
    plant_id        VARCHAR(4)   PRIMARY KEY,
    plant_name      VARCHAR(200) NOT NULL,
    bukrs           VARCHAR(4)   REFERENCES company_codes(bukrs),
    city            VARCHAR(100),
    province        VARCHAR(100),
    region          VARCHAR(20),  -- NORTH, CENTRAL, SOUTH
    plant_type      VARCHAR(30),  -- FACTORY, WAREHOUSE, OFFICE
    capacity_tons_day INTEGER,
    is_active       BOOLEAN      DEFAULT TRUE,
    created_at      TIMESTAMP    DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────
-- TABLE 3: cost_centers — Trung tâm chi phí (SAP KOSTL)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cost_centers (
    cost_center_id  VARCHAR(10)  PRIMARY KEY,
    cost_center_name VARCHAR(200) NOT NULL,
    plant_id        VARCHAR(4)   REFERENCES plants(plant_id),
    bukrs           VARCHAR(4)   REFERENCES company_codes(bukrs),
    cc_type         VARCHAR(30),  -- PRODUCTION, SALES, ADMIN, LOGISTICS, RND
    responsible     VARCHAR(100),
    is_active       BOOLEAN      DEFAULT TRUE,
    created_at      TIMESTAMP    DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────
-- TABLE 4: chart_of_accounts — Hệ thống tài khoản VAS
-- (VAS = Vietnamese Accounting Standards - Thông tư 200/2014/TT-BTC)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS chart_of_accounts (
    account_id      VARCHAR(10)  PRIMARY KEY,
    account_name    VARCHAR(200) NOT NULL,
    account_type    VARCHAR(20)  NOT NULL,  -- ASSET, LIABILITY, EQUITY, REVENUE, EXPENSE
    account_group   VARCHAR(50),
    normal_balance  CHAR(1)      NOT NULL,  -- 'D' = Debit, 'C' = Credit
    allows_cost_center BOOLEAN   DEFAULT FALSE,
    requires_partner   BOOLEAN   DEFAULT FALSE,  -- AR/AP accounts need customer/vendor
    is_reconciliation  BOOLEAN   DEFAULT FALSE,  -- Controlled account (AR/AP)
    currency        CHAR(3)      DEFAULT 'VND',
    is_active       BOOLEAN      DEFAULT TRUE,
    created_at      TIMESTAMP    DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────
-- TABLE 5: customers — Khách hàng (SAP Customer Master / KUNNR)
-- VinaMilk's distribution partners and B2B customers
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customers (
    customer_id     VARCHAR(20)  PRIMARY KEY,  -- Format: CUST_XXXXX
    customer_name   VARCHAR(300) NOT NULL,
    customer_type   VARCHAR(10)  NOT NULL,  -- MT, TT, EXPORT, GT, INTERCO
    tax_code        VARCHAR(20),
    phone           VARCHAR(20),
    email           VARCHAR(150),
    address         VARCHAR(300),
    city            VARCHAR(100),
    province        VARCHAR(100),
    country         CHAR(2)      DEFAULT 'VN',
    sales_region    VARCHAR(20),  -- NORTH, CENTRAL, SOUTH, EXPORT
    sales_channel   VARCHAR(30),  -- MODERN_TRADE, TRADITIONAL, EXPORT, GT
    credit_limit    NUMERIC(18,2),
    payment_terms   VARCHAR(10)  DEFAULT 'NET30',  -- NET15, NET30, NET60, NET90
    currency        CHAR(3)      DEFAULT 'VND',
    is_active       BOOLEAN      DEFAULT TRUE,
    created_at      TIMESTAMP    DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────
-- TABLE 6: vendors — Nhà cung cấp (SAP Vendor Master / LIFNR)
-- VinaMilk's suppliers (raw milk, packaging, services)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS vendors (
    vendor_id       VARCHAR(20)  PRIMARY KEY,  -- Format: VEND_XXXXX
    vendor_name     VARCHAR(300) NOT NULL,
    vendor_type     VARCHAR(30)  NOT NULL,  -- RAW_MATERIAL, PACKAGING, EQUIPMENT, SERVICE, LOGISTICS
    tax_code        VARCHAR(20),
    phone           VARCHAR(20),
    email           VARCHAR(150),
    address         VARCHAR(300),
    city            VARCHAR(100),
    country         CHAR(2)      DEFAULT 'VN',
    bank_name       VARCHAR(100),
    bank_account    VARCHAR(30),
    payment_terms   VARCHAR(10)  DEFAULT 'NET30',
    currency        CHAR(3)      DEFAULT 'VND',
    is_active       BOOLEAN      DEFAULT TRUE,
    created_at      TIMESTAMP    DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────
-- TABLE 7: transactions — Chứng từ kế toán (Header)
-- SAP equivalent: BKPF (Accounting Document Header)
-- This is the PRIMARY table that Debezium CDC captures
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS transactions (
    txn_id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    doc_number      VARCHAR(20)  NOT NULL,    -- SAP document number (e.g., 1800042301)
    doc_type        VARCHAR(2)   NOT NULL,    -- RV, DR, DZ, KR, KZ, SA, WA, RE
    company_code    VARCHAR(4)   DEFAULT '1000' REFERENCES company_codes(bukrs),
    fiscal_year     INTEGER      NOT NULL,
    fiscal_period   INTEGER      NOT NULL CHECK (fiscal_period BETWEEN 1 AND 16),
    posting_date    DATE         NOT NULL,    -- Date transaction is posted in SAP
    document_date   DATE         NOT NULL,    -- Date on the original document
    entry_date      TIMESTAMP    DEFAULT NOW(),
    reference       VARCHAR(50),             -- External ref (PO#, SO#, bank ref)
    header_text     VARCHAR(200),            -- Description
    currency        CHAR(3)      NOT NULL DEFAULT 'VND',
    exchange_rate   NUMERIC(10,5) DEFAULT 1.00000,  -- Rate vs VND
    total_debit     NUMERIC(18,2),
    total_credit    NUMERIC(18,2),
    -- Status field — intentionally NULL for 5% records (test DQ)
    status          VARCHAR(20)  DEFAULT 'POSTED',  -- POSTED, REVERSED, PARKED, ERROR
    created_by      VARCHAR(50),             -- SAP user login
    source_system   VARCHAR(20)  DEFAULT 'SAP_MOCK',
    reversal_doc    VARCHAR(20),             -- If this doc is a reversal
    -- Debezium CDC metadata
    _updated_at     TIMESTAMP    DEFAULT NOW()
);

-- Add trigger to update _updated_at
CREATE OR REPLACE FUNCTION update_transactions_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW._updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER transactions_updated_at
    BEFORE UPDATE ON transactions
    FOR EACH ROW EXECUTE FUNCTION update_transactions_timestamp();

-- ─────────────────────────────────────────────────────────
-- TABLE 8: general_ledger — Dòng chứng từ kế toán (Line Items)
-- SAP equivalent: BSEG (Accounting Document Line Items)
-- The actual double-entry bookkeeping lines
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS general_ledger (
    gl_id           SERIAL       PRIMARY KEY,
    txn_id          UUID         REFERENCES transactions(txn_id),
    line_item       INTEGER      NOT NULL,  -- 001, 002, 003...
    account_id      VARCHAR(10)  REFERENCES chart_of_accounts(account_id),
    debit_credit    CHAR(1)      NOT NULL CHECK (debit_credit IN ('D', 'C')),
    amount          NUMERIC(18,2) NOT NULL,  -- In transaction currency
    amount_vnd      NUMERIC(18,2),           -- Always in VND (for FX docs)
    cost_center     VARCHAR(10)  REFERENCES cost_centers(cost_center_id),
    plant           VARCHAR(4)   REFERENCES plants(plant_id),
    customer_id     VARCHAR(20)  REFERENCES customers(customer_id),
    vendor_id       VARCHAR(20)  REFERENCES vendors(vendor_id),
    tax_code        VARCHAR(5),  -- V1 (VAT 10%), V0 (VAT 0%), E (exempt)
    assignment      VARCHAR(18), -- Used for clearing reference
    item_text       VARCHAR(200),
    profit_center   VARCHAR(10),  -- Profit center for internal reporting
    created_at      TIMESTAMP    DEFAULT NOW(),
    _updated_at     TIMESTAMP    DEFAULT NOW(),
    UNIQUE (txn_id, line_item)
);

-- ─────────────────────────────────────────────────────────
-- TABLE 9: accounts_receivable — Công nợ phải thu (AR Open Items)
-- SAP equivalent: BSID (Customer Open Items) / BSAD (Cleared)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS accounts_receivable (
    ar_id           SERIAL       PRIMARY KEY,
    txn_id          UUID         REFERENCES transactions(txn_id),
    customer_id     VARCHAR(20)  NOT NULL REFERENCES customers(customer_id),
    invoice_no      VARCHAR(50)  UNIQUE,     -- VinaMilk invoice number
    invoice_date    DATE         NOT NULL,
    due_date        DATE         NOT NULL,
    amount          NUMERIC(18,2) NOT NULL,
    currency        CHAR(3)      DEFAULT 'VND',
    amount_vnd      NUMERIC(18,2),           -- VND equivalent
    paid_amount     NUMERIC(18,2) DEFAULT 0,
    overdue_days    INTEGER DEFAULT 0,
    status          VARCHAR(20)  NOT NULL DEFAULT 'OPEN',  -- OPEN, PARTIAL, PAID, OVERDUE, DISPUTED
    payment_method  VARCHAR(30),  -- BANK_TRANSFER, CASH, CREDIT_CARD, CHECK
    sales_channel   VARCHAR(10),  -- MT, TT, EXPORT, GT
    plant           VARCHAR(4)   REFERENCES plants(plant_id),
    cleared_date    DATE,
    created_at      TIMESTAMP    DEFAULT NOW(),
    updated_at      TIMESTAMP    DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION update_ar_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ar_updated_at
    BEFORE UPDATE ON accounts_receivable
    FOR EACH ROW EXECUTE FUNCTION update_ar_timestamp();

-- ─────────────────────────────────────────────────────────
-- TABLE 10: accounts_payable — Công nợ phải trả (AP Open Items)
-- SAP equivalent: BSIK (Vendor Open Items) / BSAK (Cleared)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS accounts_payable (
    ap_id           SERIAL       PRIMARY KEY,
    txn_id          UUID         REFERENCES transactions(txn_id),
    vendor_id       VARCHAR(20)  NOT NULL REFERENCES vendors(vendor_id),
    invoice_no      VARCHAR(50)  UNIQUE,
    invoice_date    DATE         NOT NULL,
    due_date        DATE         NOT NULL,
    amount          NUMERIC(18,2) NOT NULL,
    currency        CHAR(3)      DEFAULT 'VND',
    amount_vnd      NUMERIC(18,2),
    paid_amount     NUMERIC(18,2) DEFAULT 0,
    overdue_days    INTEGER DEFAULT 0,
    status          VARCHAR(20)  NOT NULL DEFAULT 'OPEN',  -- OPEN, PARTIAL, PAID, OVERDUE
    purchase_order  VARCHAR(30),  -- SAP PO number
    vendor_type     VARCHAR(30),  -- RAW_MATERIAL, PACKAGING, SERVICE, etc.
    plant           VARCHAR(4)   REFERENCES plants(plant_id),
    cleared_date    DATE,
    created_at      TIMESTAMP    DEFAULT NOW(),
    updated_at      TIMESTAMP    DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION update_ap_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ap_updated_at
    BEFORE UPDATE ON accounts_payable
    FOR EACH ROW EXECUTE FUNCTION update_ap_timestamp();

-- ─────────────────────────────────────────────────────────
-- INDEXES — For CDC performance and query optimization
-- ─────────────────────────────────────────────────────────

-- transactions indexes
CREATE INDEX idx_txn_posting_date    ON transactions (posting_date);
CREATE INDEX idx_txn_doc_type        ON transactions (doc_type);
CREATE INDEX idx_txn_company_code    ON transactions (company_code);
CREATE INDEX idx_txn_fiscal          ON transactions (fiscal_year, fiscal_period);
CREATE INDEX idx_txn_status          ON transactions (status);
CREATE INDEX idx_txn_entry_date      ON transactions (entry_date);
CREATE INDEX idx_txn_doc_number      ON transactions (doc_number);

-- general_ledger indexes
CREATE INDEX idx_gl_txn_id           ON general_ledger (txn_id);
CREATE INDEX idx_gl_account_id       ON general_ledger (account_id);
CREATE INDEX idx_gl_cost_center      ON general_ledger (cost_center);
CREATE INDEX idx_gl_customer_id      ON general_ledger (customer_id);
CREATE INDEX idx_gl_vendor_id        ON general_ledger (vendor_id);
CREATE INDEX idx_gl_created_at       ON general_ledger (created_at);

-- accounts_receivable indexes
CREATE INDEX idx_ar_customer_id      ON accounts_receivable (customer_id);
CREATE INDEX idx_ar_due_date         ON accounts_receivable (due_date);
CREATE INDEX idx_ar_status           ON accounts_receivable (status);
CREATE INDEX idx_ar_sales_channel    ON accounts_receivable (sales_channel);
CREATE INDEX idx_ar_invoice_date     ON accounts_receivable (invoice_date);

-- accounts_payable indexes
CREATE INDEX idx_ap_vendor_id        ON accounts_payable (vendor_id);
CREATE INDEX idx_ap_due_date         ON accounts_payable (due_date);
CREATE INDEX idx_ap_status           ON accounts_payable (status);
CREATE INDEX idx_ap_vendor_type      ON accounts_payable (vendor_type);


