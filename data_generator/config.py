"""
VinaMilk Data Lakehouse — Data Generator Configuration
Centralized configuration for all generator modules.
"""
import os

# ─────────────────────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB",   "vinamik_erp"),
    "user":     os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASS", "1"),
}

# ─────────────────────────────────────────────────────────
# GENERATION SPEED (records per second)
# ─────────────────────────────────────────────────────────
SPEED_CONFIGS = {
    "slow":   {"min": 1.5, "max": 2.5},   # 0.4-0.7 txn/s  → ~1.5-2.5s sleep
    "normal": {"min": 0.8, "max": 1.5},   # 0.7-1.2 txn/s  → ~0.8-1.5s sleep
    "fast":   {"min": 0.2, "max": 0.5},   # 2-5 txn/s      → ~0.2-0.5s sleep
    "burst":  {"min": 0.0, "max": 0.1},   # 10+ txn/s      → flood mode
}
DEFAULT_SPEED = "normal"

# ─────────────────────────────────────────────────────────
# BUSINESS SCENARIO WEIGHTS (must sum to 1.0)
# Reflects real VinaMilk ERP transaction volume distribution
# ─────────────────────────────────────────────────────────
SCENARIO_WEIGHTS = {
    # Core revenue: sales invoices to MT/TT channels (most frequent)
    "revenue_domestic":   0.38,  # 38% – Sales to BigC, Lotte, distributors
    "revenue_export":     0.07,  # 7%  – Export invoices in USD/EUR
    # Cash collection: customer payments
    "ar_collection":      0.15,  # 15% – Customers clearing their invoices
    # Procurement: purchasing raw materials
    "procurement_nvl":    0.12,  # 12% – Buy raw milk, sugar, additives
    "procurement_service":0.06,  # 6%  – Electricity, logistics, maintenance
    # Vendor payments
    "ap_payment":         0.07,  # 7%  – Paying suppliers
    # Periodic (end of month weighted, but still show in stream)
    "payroll":            0.04,  # 4%  – Monthly salary postings
    "depreciation":       0.03,  # 3%  – Monthly fixed asset depreciation
    # Intercompany & Finance
    "intercompany":       0.04,  # 4%  – Transfers between VinaMilk entities
    "bank_charges":       0.04,  # 4%  – Bank fees, loan interest, FX gain/loss
}

assert abs(sum(SCENARIO_WEIGHTS.values()) - 1.0) < 0.001, "Scenario weights must sum to 1.0"

# ─────────────────────────────────────────────────────────
# DATA QUALITY ERROR INJECTION RATES
# These simulate REAL errors that occur in SAP environments
# Target: ~18-22% total error rate for Great Expectations testing
# ─────────────────────────────────────────────────────────
DQ_ERROR_RATES = {
    # Most common SAP user error: forgot to fill cost center
    "missing_cost_center":  0.07,  # 7% — accounting clerk forgot CC
    # Duplicate posting: user submits twice after timeout
    "duplicate_posting":    0.03,  # 3% — double submit
    # Cleared amount posted as zero (clearing error)
    "amount_zero":          0.02,  # 2% — zero amount clearing
    # Mistyped GL account (e.g., typed 512 instead of 511)
    "invalid_gl_account":   0.02,  # 2% — wrong account number
    # Currency mismatch (pasted old template with SGD/THB)
    "wrong_currency":       0.02,  # 2% — invalid currency code
    # Future posting date (system clock drift, or pre-dated entries)
    "future_posting_date":  0.02,  # 2% — date in the future
    # Negative AR amount (credit memo posted to wrong document type)
    "negative_amount":      0.02,  # 2% — negative where positive expected
}
TOTAL_ERROR_RATE = sum(DQ_ERROR_RATES.values())  # ~20%

# ─────────────────────────────────────────────────────────
# SAP DOCUMENT NUMBER RANGES
# Each document type has its own number range in SAP
# ─────────────────────────────────────────────────────────
DOC_NUMBER_RANGES = {
    "RV": (1800000001, 1899999999),  # Revenue invoices (billing)
    "DR": (1400000001, 1499999999),  # Customer invoices (direct)
    "DZ": (1500000001, 1599999999),  # Customer payments (incoming)
    "KR": (5100000001, 5199999999),  # Vendor invoices (received)
    "KZ": (5200000001, 5299999999),  # Vendor payments (outgoing)
    "SA": (1900000001, 1999999999),  # G/L account documents (SA/payroll/depreciation)
    "WA": (4900000001, 4999999999),  # Goods movements (GR/GI)
    "RE": (5300000001, 5399999999),  # Invoice receipt (MIRO)
    "AB": (9900000001, 9999999999),  # Accounting adjustments/reversals
}

# ─────────────────────────────────────────────────────────
# FISCAL YEAR SETTINGS
# VinaMilk follows Vietnamese fiscal year: January–December
# ─────────────────────────────────────────────────────────
FISCAL_YEAR = 2024
COMPANY_CODE = "1000"  # VinaMilk HQ

# FX Rates (VND per 1 foreign currency unit)
# Updated when fx_rate_producer runs — these are fallback defaults
DEFAULT_FX_RATES = {
    "USD": 25150.0,    # 1 USD = 25,150 VND
    "EUR": 27200.0,    # 1 EUR = 27,200 VND
    "JPY": 167.0,      # 1 JPY = 167 VND
    "SGD": 18750.0,    # 1 SGD = 18,750 VND
    "VND": 1.0,
}

# ─────────────────────────────────────────────────────────
# VINAMIILK PRODUCT LINES (for revenue simulation)
# ─────────────────────────────────────────────────────────
PRODUCT_LINES = {
    "UHT_MILK": {
        "account": "5111",
        "name": "Sữa tươi 100% Vinamilk UHT",
        "unit_price_vnd": (25000, 650000),   # Per case (min, max)
        "channels": ["MT", "TT", "GT"],
        "weight": 0.40,   # 40% of revenue
    },
    "CONDENSED_MILK": {
        "account": "5112",
        "name": "Sữa đặc Ông Thọ / Ngôi Sao Phương Nam",
        "unit_price_vnd": (18000, 420000),
        "channels": ["MT", "TT"],
        "weight": 0.20,
    },
    "BABY_FORMULA": {
        "account": "5113",
        "name": "Sữa bột Dielac Alpha / Grow",
        "unit_price_vnd": (280000, 5600000),
        "channels": ["MT", "GT"],
        "weight": 0.20,
    },
    "YOGURT": {
        "account": "5114",
        "name": "Sữa chua Vinamilk / ProYogurt",
        "unit_price_vnd": (12000, 380000),
        "channels": ["MT", "TT", "GT"],
        "weight": 0.12,
    },
    "ICE_CREAM_JUICE": {
        "account": "5115",
        "name": "Kem Thủy Tạ / Nước trái cây Vfresh",
        "unit_price_vnd": (8000, 250000),
        "channels": ["MT", "GT"],
        "weight": 0.08,
    },
}

# ─────────────────────────────────────────────────────────
# LOGGING CONFIGURATION
# ─────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# ─────────────────────────────────────────────────────────
# MISA CSV Producer settings
# ─────────────────────────────────────────────────────────
MISA_CONFIG = {
    "csv_dir": os.getenv("MISA_CSV_DIR", "./data/misa_exports"),
    "kafka_topic": "erp.misa_invoices",
    "kafka_bootstrap": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
    "batch_time": "06:00",  # Run at 6 AM daily
    "subsidiary_codes": ["1100", "1200"],  # Dalat Milk, Moc Chau
}

# ─────────────────────────────────────────────────────────
# FX Rate Producer settings
# ─────────────────────────────────────────────────────────
FX_CONFIG = {
    "kafka_topic": "erp.fx_rates",
    "kafka_bootstrap": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
    "api_key": os.getenv("EXCHANGE_RATE_API_KEY", ""),
    "api_url": "https://v6.exchangerate-api.com/v6/{api_key}/latest/VND",
    "poll_interval_seconds": 3600,  # Every hour
    "currencies": ["USD", "EUR", "JPY", "SGD"],
}
