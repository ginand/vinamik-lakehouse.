# ─────────────────────────────────────────────────────────
# VinaMilk Data Lakehouse — Silver Layer Batch Processing
# ─────────────────────────────────────────────────────────
# Bronze (raw Debezium CDC JSON) → Silver (clean, typed, deduplicated)
#
# Chạy định kỳ mỗi RUN_INTERVAL_MINUTES phút (mặc định: 5).
# Dùng Delta Lake MERGE để xử lý CDC (INSERT / UPDATE / DELETE).
#
# Bảng xử lý:
#   transactions → transactions_silver
#   general_ledger → general_ledger_silver
#   accounts_receivable → ar_silver
#   accounts_payable → ap_silver
#   customers → customers_silver
#   vendors → vendors_silver
#   fx_rates → fx_rates_silver
# ─────────────────────────────────────────────────────────

import os
import sys
import time
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit, to_date, to_timestamp,
    when, coalesce, datediff, abs as spark_abs, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, BooleanType, TimestampType
)
from delta.tables import DeltaTable

# ─────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("vinamik.silver_batch")
logging.getLogger("py4j").setLevel(logging.WARNING)

# ─────────────────────────────────────────────────────────
# ENV CONFIG
# ─────────────────────────────────────────────────────────
def _env(key, default=""):
    val = os.getenv(key, default)
    return val.strip('"').strip("'") if val else val

# Khi chạy unit test (PYTEST_CURRENT_TEST được set tự động bởi pytest)
# thì KHÔNG cần kết nối ADLS thực, dùng path local.
_TESTING = os.getenv("PYTEST_CURRENT_TEST") is not None or os.getenv("CI_TESTING") == "1"

STORAGE_ACCOUNT   = _env("AZURE_STORAGE_ACCOUNT_NAME", "vmlakehouse2024")
STORAGE_KEY       = _env("AZURE_STORAGE_ACCOUNT_KEY")
RUN_INTERVAL_MIN  = int(_env("RUN_INTERVAL_MINUTES", "15"))

if _TESTING:
    # Dùng path local tạm trong unit test
    _BASE = "/tmp/vinamik_test"
    BRONZE_PATH     = f"{_BASE}/bronze"
    SILVER_PATH     = f"{_BASE}/silver"
    QUARANTINE_PATH = f"{_BASE}/quarantine"
    CHECKPOINT_PATH = f"{_BASE}/checkpoints"
else:
    BRONZE_PATH     = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
    SILVER_PATH     = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
    QUARANTINE_PATH = f"abfss://quarantine@{STORAGE_ACCOUNT}.dfs.core.windows.net"
    CHECKPOINT_PATH = f"abfss://checkpoints@{STORAGE_ACCOUNT}.dfs.core.windows.net"
    if not STORAGE_KEY:
        logger.error("❌ Thiếu AZURE_STORAGE_ACCOUNT_KEY — kiểm tra .env")
        sys.exit(1)

# ─────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────
def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("VinaMilk-Silver-Batch")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4") # FIX OOM: Default is 200, which uses too much memory for local micro-batches
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set(
        f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        STORAGE_KEY
    )
    return spark


# ─────────────────────────────────────────────────────────
# DEBEZIUM CDC SCHEMAS (per table)
# Maps PostgreSQL columns → PySpark types
# ─────────────────────────────────────────────────────────

TXN_PAYLOAD_SCHEMA = StructType([
    StructField("txn_id",         StringType()),
    StructField("doc_number",     StringType()),
    StructField("doc_type",       StringType()),
    StructField("company_code",   StringType()),
    StructField("fiscal_year",    IntegerType()),
    StructField("fiscal_period",  IntegerType()),
    StructField("posting_date",   IntegerType()),   # Debezium sends DATE as epoch days
    StructField("document_date",  IntegerType()),
    StructField("entry_date",     LongType()),      # TIMESTAMP as epoch micros
    StructField("reference",      StringType()),
    StructField("header_text",    StringType()),
    StructField("currency",       StringType()),
    StructField("exchange_rate",  DoubleType()),
    StructField("total_debit",    DoubleType()),
    StructField("total_credit",   DoubleType()),
    StructField("status",         StringType()),
    StructField("created_by",     StringType()),
    StructField("source_system",  StringType()),
    StructField("reversal_doc",   StringType()),
    StructField("_updated_at",    LongType()),
])

GL_PAYLOAD_SCHEMA = StructType([
    StructField("gl_id",          IntegerType()),
    StructField("txn_id",         StringType()),
    StructField("line_item",      IntegerType()),
    StructField("account_id",     StringType()),
    StructField("debit_credit",   StringType()),
    StructField("amount",         DoubleType()),
    StructField("amount_vnd",     DoubleType()),
    StructField("cost_center",    StringType()),
    StructField("plant",          StringType()),
    StructField("customer_id",    StringType()),
    StructField("vendor_id",      StringType()),
    StructField("tax_code",       StringType()),
    StructField("assignment",     StringType()),
    StructField("item_text",      StringType()),
    StructField("profit_center",  StringType()),
    StructField("created_at",     LongType()),
    StructField("_updated_at",    LongType()),
])

AR_PAYLOAD_SCHEMA = StructType([
    StructField("ar_id",          IntegerType()),
    StructField("txn_id",         StringType()),
    StructField("customer_id",    StringType()),
    StructField("invoice_no",     StringType()),
    StructField("invoice_date",   IntegerType()),
    StructField("due_date",       IntegerType()),
    StructField("amount",         DoubleType()),
    StructField("currency",       StringType()),
    StructField("amount_vnd",     DoubleType()),
    StructField("paid_amount",    DoubleType()),
    StructField("overdue_days",   IntegerType()),
    StructField("status",         StringType()),
    StructField("payment_method", StringType()),
    StructField("sales_channel",  StringType()),
    StructField("plant",          StringType()),
    StructField("cleared_date",   IntegerType()),
    StructField("created_at",     LongType()),
    StructField("updated_at",     LongType()),
])

AP_PAYLOAD_SCHEMA = StructType([
    StructField("ap_id",          IntegerType()),
    StructField("txn_id",         StringType()),
    StructField("vendor_id",      StringType()),
    StructField("invoice_no",     StringType()),
    StructField("invoice_date",   IntegerType()),
    StructField("due_date",       IntegerType()),
    StructField("amount",         DoubleType()),
    StructField("currency",       StringType()),
    StructField("amount_vnd",     DoubleType()),
    StructField("paid_amount",    DoubleType()),
    StructField("overdue_days",   IntegerType()),
    StructField("status",         StringType()),
    StructField("purchase_order", StringType()),
    StructField("vendor_type",    StringType()),
    StructField("plant",          StringType()),
    StructField("cleared_date",   IntegerType()),
    StructField("created_at",     LongType()),
    StructField("updated_at",     LongType()),
])

CUSTOMER_PAYLOAD_SCHEMA = StructType([
    StructField("customer_id",    StringType()),
    StructField("customer_name",  StringType()),
    StructField("customer_type",  StringType()),
    StructField("tax_code",       StringType()),
    StructField("phone",          StringType()),
    StructField("email",          StringType()),
    StructField("address",        StringType()),
    StructField("city",           StringType()),
    StructField("province",       StringType()),
    StructField("country",        StringType()),
    StructField("sales_region",   StringType()),
    StructField("sales_channel",  StringType()),
    StructField("credit_limit",   DoubleType()),
    StructField("payment_terms",  StringType()),
    StructField("currency",       StringType()),
    StructField("is_active",      BooleanType()),
    StructField("created_at",     LongType()),
])

VENDOR_PAYLOAD_SCHEMA = StructType([
    StructField("vendor_id",      StringType()),
    StructField("vendor_name",    StringType()),
    StructField("vendor_type",    StringType()),
    StructField("tax_code",       StringType()),
    StructField("phone",          StringType()),
    StructField("email",          StringType()),
    StructField("address",        StringType()),
    StructField("city",           StringType()),
    StructField("country",        StringType()),
    StructField("bank_name",      StringType()),
    StructField("bank_account",   StringType()),
    StructField("payment_terms",  StringType()),
    StructField("currency",       StringType()),
    StructField("is_active",      BooleanType()),
    StructField("created_at",     LongType()),
])

FX_PAYLOAD_SCHEMA = StructType([
    # Tên field khớp với fx_rate_producer.py → publish_rates()
    StructField("currency_pair",     StringType()),   # "VND/USD"
    StructField("base_currency",     StringType()),   # "VND"
    StructField("quote_currency",    StringType()),   # "USD" ← đây là 'currency' trong silver
    StructField("vnd_per_unit",      DoubleType()),   # ← đây là 'rate_vnd' trong silver
    StructField("rate_date",         StringType()),   # "2026-06-22" ← effective_date
    StructField("rate_time",         StringType()),
    StructField("timestamp",         StringType()),   # ← fetched_at
    StructField("bank_buying_rate",  DoubleType()),
    StructField("bank_selling_rate", DoubleType()),
    StructField("sbv_reference",     DoubleType()),
    StructField("deviation_pct",     DoubleType()),
    StructField("source",            StringType()),
    StructField("fiscal_year",       IntegerType()),
    StructField("fiscal_period",     IntegerType()),
])

BUDGET_PAYLOAD_SCHEMA = StructType([
    StructField("budget_id", StringType()),
    StructField("fiscal_year", IntegerType()),
    StructField("month", IntegerType()),
    StructField("cost_center", StringType()),
    StructField("department", StringType()),
    StructField("account_code", StringType()),
    StructField("account_name", StringType()),
    StructField("product_group", StringType()),
    StructField("budget_amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("approved_by", StringType()),
    StructField("status", StringType()),
    StructField("updated_date", StringType()),
    StructField("version", IntegerType()),
    StructField("source", StringType())
])

# ─────────────────────────────────────────────────────────
# HELPER: Parse Debezium CDC envelope
# ─────────────────────────────────────────────────────────
def parse_cdc(df: DataFrame, payload_schema: StructType, after_col: str = "after") -> DataFrame:
    """
    Debezium CDC connector dùng ExtractNewRecordState (unwrap transform) nên payload
    đã được flatten ra — không có lớp bọc "payload" nữa. Raw JSON có dạng:
      { ...data_fields..., "__op": "c", "__deleted": "false", "__ts_ms": 123 }

    Nếu connector KHÔNG dùng unwrap (raw envelope), format là:
      { "schema": {...}, "payload": { ...data + meta... } }

    Hàm này tự nhận diện: nếu $.payload tồn tại → envelope, ngược lại → flat unwrapped.
    """
    from pyspark.sql.functions import get_json_object

    # Full schema = data fields + Debezium metadata fields thêm bởi add.fields config
    payload_with_meta = StructType(payload_schema.fields + [
        StructField("__op",            StringType()),
        StructField("__table",         StringType()),
        StructField("__ts_ms",         LongType()),
        StructField("__source_ts_ms",  LongType()),
        StructField("__deleted",       StringType()),
    ])

    # Kiểm tra format: thử extract $.payload — nếu null thì là flat (unwrapped)
    df_detected = df.withColumn("_probe", get_json_object(col("raw_payload"), "$.payload"))

    # Phân luồng: envelope vs flat
    envelope_df = df_detected.filter(col("_probe").isNotNull())
    flat_df     = df_detected.filter(col("_probe").isNull())

    # --- Xử lý envelope (có lớp bọc "payload") ---
    parsed_envelope = (
        envelope_df
        .withColumn("_p", from_json(col("_probe"), payload_with_meta))
        .filter(col("_p").isNotNull())
        .withColumn("_cdc_op",     coalesce(col("_p.__op"), lit("c")))
        .withColumn("_cdc_ts_ms",  col("_p.__ts_ms"))
        .withColumn("_is_deleted", (col("_p.__deleted") == "true") | (col("_p.__op") == "d"))
        .select("_cdc_op", "_cdc_ts_ms", "_is_deleted", "_p.*")
        .drop("__op", "__table", "__ts_ms", "__source_ts_ms", "__deleted")
        .withColumn("_silver_loaded_at", current_timestamp())
    )

    # --- Xử lý flat / unwrapped (ExtractNewRecordState đã flatten) ---
    parsed_flat = (
        flat_df
        .withColumn("_p", from_json(col("raw_payload"), payload_with_meta))
        .filter(col("_p").isNotNull())
        .withColumn("_cdc_op",     coalesce(col("_p.__op"), lit("c")))
        .withColumn("_cdc_ts_ms",  col("_p.__ts_ms"))
        .withColumn("_is_deleted", (col("_p.__deleted") == "true") | (col("_p.__op") == "d"))
        .select("_cdc_op", "_cdc_ts_ms", "_is_deleted", "_p.*")
        .drop("__op", "__table", "__ts_ms", "__source_ts_ms", "__deleted")
        .withColumn("_silver_loaded_at", current_timestamp())
    )

    return parsed_envelope.unionByName(parsed_flat)


def epoch_days_to_date(col_name: str):
    """Debezium encodes DATE as integer (days since 1970-01-01)."""
    return expr(f"date_add(date '1970-01-01', `{col_name}`)")


def epoch_micros_to_ts(col_name: str):
    """Debezium encodes TIMESTAMP as microseconds since epoch."""
    return (col(col_name) / 1_000_000).cast(TimestampType())


# ─────────────────────────────────────────────────────────
# SILVER TRANSFORMS — one function per table
# ─────────────────────────────────────────────────────────

def transform_transactions(df: DataFrame) -> DataFrame:
    parsed = parse_cdc(df, TXN_PAYLOAD_SCHEMA)
    return (
        parsed
        .withColumn("posting_date",  epoch_days_to_date("posting_date"))
        .withColumn("document_date", epoch_days_to_date("document_date"))
        .withColumn("entry_date",    epoch_micros_to_ts("entry_date"))
        .withColumn("_updated_at",   epoch_micros_to_ts("_updated_at"))
        # DQ flags
        .withColumn("dq_missing_cost_center", lit(False))   # GL level, not TXN
        .withColumn("dq_future_posting",
            col("posting_date") > current_timestamp().cast("date"))
        .withColumn("dq_amount_zero",
            (col("total_debit") == 0) & (col("total_credit") == 0))
        .withColumn("dq_wrong_currency",
            ~col("currency").isin("VND", "USD", "EUR", "JPY", "SGD"))
        .withColumn("dq_null_company_code", col("company_code").isNull())
        .withColumn("dq_null_txn_id", col("txn_id").isNull())
        .withColumn("dq_invalid_status", ~col("status").isin("POSTED", "DRAFT", "VOID"))
        .withColumn("dq_negative_amount_txn", (col("total_debit") < 0) | (col("total_credit") < 0))
        .withColumn("dq_is_clean",
            ~col("dq_future_posting") & ~col("dq_amount_zero") & ~col("dq_wrong_currency") &
            ~col("dq_null_company_code") & ~col("dq_null_txn_id") &
            ~col("dq_invalid_status") & ~col("dq_negative_amount_txn"))
        # Partition column
        .withColumn("posting_month", col("posting_date").cast(StringType()).substr(1, 7))
    )


def transform_general_ledger(df: DataFrame) -> DataFrame:
    parsed = parse_cdc(df, GL_PAYLOAD_SCHEMA)
    return (
        parsed
        .withColumn("created_at",  epoch_micros_to_ts("created_at"))
        .withColumn("_updated_at", epoch_micros_to_ts("_updated_at"))
        # DQ flags
        .withColumn("dq_missing_cost_center", col("cost_center").isNull())
        .withColumn("dq_amount_zero", col("amount") == 0)
        .withColumn("dq_negative_amount",
            (col("debit_credit") == "D") & (col("amount") < 0))
        .withColumn("dq_is_clean",
            ~col("dq_missing_cost_center") & ~col("dq_amount_zero") & ~col("dq_negative_amount"))
        .withColumn("created_month", col("created_at").cast(StringType()).substr(1, 7))
    )


def transform_ar(df: DataFrame) -> DataFrame:
    parsed = parse_cdc(df, AR_PAYLOAD_SCHEMA)
    return (
        parsed
        .withColumn("invoice_date", epoch_days_to_date("invoice_date"))
        .withColumn("due_date",     epoch_days_to_date("due_date"))
        .withColumn("cleared_date", epoch_days_to_date("cleared_date"))
        .withColumn("created_at",   epoch_micros_to_ts("created_at"))
        .withColumn("updated_at",   epoch_micros_to_ts("updated_at"))
        .withColumn("outstanding_amount", col("amount") - coalesce(col("paid_amount"), lit(0.0)))
        .withColumn("outstanding_vnd",    col("amount_vnd") - coalesce(col("paid_amount"), lit(0.0)))
        # Aging bucket (from overdue_days or compute from due_date)
        .withColumn("aging_bucket",
            when(col("status").isin("PAID"), lit("PAID"))
            .when(col("overdue_days") <= 0,  lit("CURRENT"))
            .when(col("overdue_days") <= 30, lit("1_30_DAYS"))
            .when(col("overdue_days") <= 60, lit("31_60_DAYS"))
            .when(col("overdue_days") <= 90, lit("61_90_DAYS"))
            .otherwise(lit("OVER_90_DAYS"))
        )
        # DQ flags — Kiểm tra chất lượng dữ liệu công nợ phải thu
        .withColumn("dq_null_customer",  col("customer_id").isNull())
        .withColumn("dq_negative_amount", col("amount") < 0)
        .withColumn("dq_invalid_status",
            ~col("status").isin("OPEN", "PARTIAL", "PAID", "OVERDUE", "DISPUTED"))
        .withColumn("dq_future_invoice",
            col("invoice_date") > current_timestamp().cast("date"))
        .withColumn("dq_paid_exceeds_amount",
            coalesce(col("paid_amount"), lit(0.0)) > col("amount"))
        .withColumn("dq_is_clean",
            ~col("dq_null_customer") & ~col("dq_negative_amount")
            & ~col("dq_invalid_status") & ~col("dq_future_invoice")
            & ~col("dq_paid_exceeds_amount"))
        .withColumn("invoice_month", col("invoice_date").cast(StringType()).substr(1, 7))
    )


def transform_ap(df: DataFrame) -> DataFrame:
    parsed = parse_cdc(df, AP_PAYLOAD_SCHEMA)
    return (
        parsed
        .withColumn("invoice_date", epoch_days_to_date("invoice_date"))
        .withColumn("due_date",     epoch_days_to_date("due_date"))
        .withColumn("cleared_date", epoch_days_to_date("cleared_date"))
        .withColumn("created_at",   epoch_micros_to_ts("created_at"))
        .withColumn("updated_at",   epoch_micros_to_ts("updated_at"))
        .withColumn("outstanding_amount", col("amount") - coalesce(col("paid_amount"), lit(0.0)))
        .withColumn("outstanding_vnd",    col("amount_vnd") - coalesce(col("paid_amount"), lit(0.0)))
        .withColumn("aging_bucket",
            when(col("status").isin("PAID"), lit("PAID"))
            .when(col("overdue_days") <= 0,  lit("CURRENT"))
            .when(col("overdue_days") <= 30, lit("1_30_DAYS"))
            .when(col("overdue_days") <= 60, lit("31_60_DAYS"))
            .when(col("overdue_days") <= 90, lit("61_90_DAYS"))
            .otherwise(lit("OVER_90_DAYS"))
        )
        # DQ flags — Kiểm tra chất lượng dữ liệu công nợ phải trả
        .withColumn("dq_null_vendor",    col("vendor_id").isNull())
        .withColumn("dq_negative_amount", col("amount") < 0)
        .withColumn("dq_invalid_status",
            ~col("status").isin("OPEN", "PARTIAL", "PAID", "OVERDUE"))
        .withColumn("dq_future_invoice",
            col("invoice_date") > current_timestamp().cast("date"))
        .withColumn("dq_paid_exceeds_amount",
            coalesce(col("paid_amount"), lit(0.0)) > col("amount"))
        .withColumn("dq_is_clean",
            ~col("dq_null_vendor") & ~col("dq_negative_amount")
            & ~col("dq_invalid_status") & ~col("dq_future_invoice")
            & ~col("dq_paid_exceeds_amount"))
        .withColumn("invoice_month", col("invoice_date").cast(StringType()).substr(1, 7))
    )


def transform_customers(df: DataFrame) -> DataFrame:
    parsed = parse_cdc(df, CUSTOMER_PAYLOAD_SCHEMA)
    return parsed.withColumn("created_at", epoch_micros_to_ts("created_at"))


def transform_vendors(df: DataFrame) -> DataFrame:
    parsed = parse_cdc(df, VENDOR_PAYLOAD_SCHEMA)
    return parsed.withColumn("created_at", epoch_micros_to_ts("created_at"))


def transform_fx_rates(df: DataFrame) -> DataFrame:
    """FX rates come from custom producer (flat JSON, no CDC envelope).
    No Debezium CDC wrapper → no _is_deleted / _cdc_op fields.
    Map producer field names → silver column names:
      quote_currency → currency
      vnd_per_unit   → rate_vnd
      rate_date      → effective_date
      timestamp      → fetched_at
    """
    return (
        df
        .withColumn("_fx", from_json(col("raw_payload"), FX_PAYLOAD_SCHEMA))
        .filter(col("_fx").isNotNull())
        .select(
            col("_fx.quote_currency").alias("currency"),        # USD, EUR, JPY, SGD
            col("_fx.vnd_per_unit").alias("rate_vnd"),          # VND per 1 unit
            col("_fx.rate_date").alias("effective_date"),        # YYYY-MM-DD
            col("_fx.timestamp").alias("fetched_at"),           # ISO timestamp
            col("_fx.source").alias("source"),                  # SBV_MOCK / EXCHANGERATE_API
            col("_fx.bank_buying_rate").alias("bank_buying_rate"),
            col("_fx.bank_selling_rate").alias("bank_selling_rate"),
            col("_fx.sbv_reference").alias("sbv_reference"),
            col("_fx.deviation_pct").alias("deviation_pct"),
            col("_fx.fiscal_year").alias("fiscal_year"),
            col("_fx.fiscal_period").alias("fiscal_period"),
            current_timestamp().alias("_silver_loaded_at"),
            lit(False).alias("_is_deleted"),   # FX producer không có CDC delete
        )
        .filter(col("currency").isNotNull() & col("rate_vnd").isNotNull())
    )

def transform_budget_plan(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("_bp", from_json(col("raw_payload"), BUDGET_PAYLOAD_SCHEMA))
        .filter(col("_bp").isNotNull())
        .select(
            col("_bp.budget_id").alias("budget_id"),
            col("_bp.fiscal_year").alias("fiscal_year"),
            col("_bp.month").alias("month"),
            col("_bp.cost_center").alias("cost_center"),
            col("_bp.department").alias("department"),
            col("_bp.account_code").alias("account_code"),
            col("_bp.account_name").alias("account_name"),
            col("_bp.product_group").alias("product_group"),
            col("_bp.budget_amount").alias("budget_amount"),
            col("_bp.currency").alias("currency"),
            col("_bp.approved_by").alias("approved_by"),
            col("_bp.status").alias("status"),
            col("_bp.updated_date").alias("updated_date"),
            lit(False).alias("_is_deleted"),
            current_timestamp().alias("_silver_loaded_at")
        )
    )


# ─────────────────────────────────────────────────────────
# DATA QUALITY — Quarantine Logic
# Tách records lỗi (dq_is_clean = FALSE) ra khỏi Silver,
# ghi vào bảng quarantine với metadata đầy đủ để debug.
# ─────────────────────────────────────────────────────────

# Map DQ flag columns → (error_type, error_column)
DQ_FLAG_MAP = {
    # Transactions
    "dq_future_posting":      ("FUTURE_POSTING_DATE",  "posting_date"),
    "dq_amount_zero":         ("AMOUNT_ZERO",          "total_debit/total_credit"),
    "dq_wrong_currency":      ("INVALID_CURRENCY",     "currency"),
    "dq_null_company_code":   ("NULL_COMPANY_CODE",    "company_code"),
    "dq_null_txn_id":         ("NULL_TXN_ID",          "txn_id"),
    "dq_invalid_status":      ("INVALID_STATUS",       "status"),
    "dq_negative_amount_txn": ("NEGATIVE_AMOUNT",      "total_debit/total_credit"),
    # General Ledger
    "dq_missing_cost_center": ("MISSING_COST_CENTER",  "cost_center"),
    "dq_negative_amount":     ("NEGATIVE_AMOUNT",      "amount"),
    # AR
    "dq_null_customer":       ("NULL_CUSTOMER_ID",     "customer_id"),
    "dq_invalid_status":      ("INVALID_STATUS",       "status"),
    "dq_future_invoice":      ("FUTURE_INVOICE_DATE",  "invoice_date"),
    "dq_paid_exceeds_amount": ("PAID_EXCEEDS_AMOUNT",  "paid_amount"),
    # AP
    "dq_null_vendor":         ("NULL_VENDOR_ID",       "vendor_id"),
}


def quarantine_and_filter(
    spark: SparkSession,
    silver_df: DataFrame,
    source_table: str,
    pk_col: str,
) -> tuple:
    """
    Tách records lỗi (dq_is_clean = FALSE) ra khỏi DataFrame.
    Records lỗi được ghi vào bảng quarantine trên ADLS Gen2
    với metadata đầy đủ: _error_type, _error_column, _quarantined_at, _source_table.

    Returns: (clean_df, quarantine_count)
    """
    # Nếu bảng không có DQ flags (customers, vendors, fx_rates) → trả về nguyên bản
    if "dq_is_clean" not in silver_df.columns:
        return silver_df, 0

    clean_df      = silver_df.filter(col("dq_is_clean") == True)
    quarantine_df = silver_df.filter(col("dq_is_clean") == False)

    q_count = quarantine_df.count()
    if q_count == 0:
        return clean_df, 0

    # ── Xác định loại lỗi từ DQ flags ──
    # Ưu tiên lỗi nghiêm trọng trước (first match)
    error_type_expr = lit("UNKNOWN")
    error_col_expr  = lit("unknown")

    for flag_col, (err_type, err_column) in DQ_FLAG_MAP.items():
        if flag_col in quarantine_df.columns:
            error_type_expr = when(col(flag_col) == True, lit(err_type)).otherwise(error_type_expr)
            error_col_expr  = when(col(flag_col) == True, lit(err_column)).otherwise(error_col_expr)

    quarantine_with_meta = (
        quarantine_df
        .withColumn("_error_type",     error_type_expr)
        .withColumn("_error_column",   error_col_expr)
        .withColumn("_quarantined_at", current_timestamp())
        .withColumn("_source_table",   lit(source_table))
    )

    # ── Ghi vào quarantine container trên ADLS Gen2 (APPEND) ──
    quarantine_path = f"{QUARANTINE_PATH}/{source_table}"
    try:
        (
            quarantine_with_meta.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(quarantine_path)
        )
        logger.info(f"    🔴 {q_count} records → quarantine/{source_table}")
    except Exception as e:
        # Quarantine ghi thất bại không nên block pipeline chính
        logger.warning(f"    ⚠️  Không thể ghi quarantine/{source_table}: {e}")
        logger.warning(f"    (Records lỗi vẫn bị loại khỏi Silver, nhưng không lưu quarantine)")

    return clean_df, q_count


# ─────────────────────────────────────────────────────────
# MERGE INTO SILVER (UPSERT for CDC correctness)
# ─────────────────────────────────────────────────────────
_TABLE_NOT_FOUND_HINTS = (
    "is not a Delta table",
    "doesn't exist",
    "Path does not exist",
    "No such file or directory",
    "DELTA_TABLE_NOT_FOUND",
    "DELTA_MISSING_TRANSACTION_LOG",
)

def merge_into_silver(
    spark: SparkSession,
    new_df: DataFrame,
    silver_table_path: str,
    pk_col: str,
    partition_col: str = None
):
    """
    MERGE new_df into Silver Delta table at silver_table_path.
    Handles INSERT, UPDATE, and soft-DELETE via _is_deleted flag.
    Chỉ tạo mới bảng khi bảng thực sự chưa tồn tại — không nuốt lỗi khác.
    """
    try:
        existing = DeltaTable.forPath(spark, silver_table_path)
        (
            existing.alias("s")
            .merge(
                new_df.alias("n"),
                f"s.{pk_col} = n.{pk_col}"
            )
            .whenMatchedUpdate(
                condition="n._is_deleted = true",
                set={"_is_deleted": lit(True), "_silver_loaded_at": current_timestamp()}
            )
            .whenMatchedUpdateAll(condition="n._is_deleted = false")
            .whenNotMatchedInsertAll()
            .execute()
        )
    except Exception as exc:
        err_msg = str(exc)
        if any(hint in err_msg for hint in _TABLE_NOT_FOUND_HINTS):
            # Bảng chưa tồn tại → tạo mới
            logger.info(f"    ℹ️  Silver table chưa có → tạo mới: {silver_table_path}")
            writer = new_df.write.format("delta").mode("overwrite")
            if partition_col:
                writer = writer.partitionBy(partition_col)
            writer.save(silver_table_path)
        else:
            # Lỗi thực sự (schema mismatch, ADLS auth, v.v.) → re-raise
            raise


# ─────────────────────────────────────────────────────────
# PIPELINE CONFIG — (bronze_table, transform_fn, silver_table, pk)
# ─────────────────────────────────────────────────────────
PIPELINE = [
    {
        "bronze":    "transactions_bronze",
        "silver":    "transactions_silver",
        "transform": transform_transactions,
        "pk":        "txn_id",
        "partition": "posting_month",
    },
    {
        "bronze":    "general_ledger_bronze",
        "silver":    "general_ledger_silver",
        "transform": transform_general_ledger,
        "pk":        "gl_id",
        "partition": "created_month",
    },
    {
        "bronze":    "ar_bronze",
        "silver":    "ar_silver",
        "transform": transform_ar,
        "pk":        "ar_id",
        "partition": "invoice_month",
    },
    {
        "bronze":    "ap_bronze",
        "silver":    "ap_silver",
        "transform": transform_ap,
        "pk":        "ap_id",
        "partition": "invoice_month",
    },
    {
        "bronze":    "customers_bronze",
        "silver":    "customers_silver",
        "transform": transform_customers,
        "pk":        "customer_id",
        "partition": None,
    },
    {
        "bronze":    "vendors_bronze",
        "silver":    "vendors_silver",
        "transform": transform_vendors,
        "pk":        "vendor_id",
        "partition": None,
    },
    {
        "bronze":    "fx_rates_bronze",
        "silver":    "fx_rates_silver",
        "transform": transform_fx_rates,
        "pk":        "currency",
        "partition": None,
    },
    {
        "bronze":    "budget_plan_bronze",
        "silver":    "budget_plan_silver",
        "transform": transform_budget_plan,
        "pk":        "budget_id",
        "partition": "month",
    }
]


# ─────────────────────────────────────────────────────────
# MAIN BATCH LOOP
# ─────────────────────────────────────────────────────────
def _bronze_table_is_ready(spark: SparkSession, bronze_path: str, table_name: str) -> bool:
    """
    Kiểm tra Bronze Delta table đã tồn tại và có ít nhất 1 record chưa.
    Tránh lỗi DELTA_SCHEMA_NOT_SET khi table còn rỗng hoặc chưa được tạo.
    """
    try:
        count = spark.read.format("delta").load(bronze_path).limit(1).count()
        if count == 0:
            logger.info(f"  ⏭  {table_name}: Bronze table rỗng — bỏ qua lần này")
            return False
        return True
    except Exception as e:
        err_msg = str(e)
        schema_errors = (
            "DELTA_SCHEMA_NOT_SET",
            "Table schema is not set",
            "Path does not exist",
            "is not a Delta table",
            "doesn't exist",
            "No such file or directory",
        )
        if any(hint in err_msg for hint in schema_errors):
            logger.info(f"  ⏭  {table_name}: Bronze table chưa có dữ liệu ({type(e).__name__}) — bỏ qua")
            return False
        # Lỗi khác (auth, network) → re-raise
        raise


def run_silver_batch(spark: SparkSession):
    run_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info(f"═══════ Silver Incremental Batch Run @ {run_ts} ═══════")
    success, skipped, failed = 0, 0, 0

    def process_cfg(cfg):
        bronze_path = f"{BRONZE_PATH}/{cfg['bronze']}"
        silver_path = f"{SILVER_PATH}/{cfg['silver']}"
        chk_dir = f"{CHECKPOINT_PATH}/silver_incremental_v2_{cfg['silver']}"

        if not _bronze_table_is_ready(spark, bronze_path, cfg['bronze']):
            return None, cfg['silver']

        def process_micro_batch(micro_df: DataFrame, batch_id: int):
            silver_df = cfg["transform"](micro_df)
            
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number, col, desc
            sort_col = "_cdc_ts_ms" if "_cdc_ts_ms" in silver_df.columns else "_silver_loaded_at"
            windowSpec = Window.partitionBy(cfg["pk"]).orderBy(desc(sort_col))
            silver_df = silver_df.withColumn("_rn", row_number().over(windowSpec)) \
                                 .filter(col("_rn") == 1) \
                                 .drop("_rn")
                                 
            silver_df.persist()
            try:
                has_deleted_col = "_is_deleted" in silver_df.columns
                if has_deleted_col:
                    active_df  = silver_df.filter(col("_is_deleted") == False)
                    deleted_df = silver_df.filter(col("_is_deleted") == True)
                else:
                    active_df  = silver_df
                    deleted_df = silver_df.filter(lit(False))

                del_count = deleted_df.count()

                # ── Data Quality: tách records lỗi vào quarantine ──
                clean_df, q_count = quarantine_and_filter(
                    spark, active_df, cfg["silver"], cfg["pk"]
                )

                out_count = clean_df.count()
                in_count  = out_count + del_count + q_count

                if in_count > 0:
                    logger.info(f"    [Batch {batch_id}] {in_count} records → {cfg['silver']}")
                    # Merge: clean records + deleted records (soft-delete)
                    merge_df = clean_df.unionByName(deleted_df, allowMissingColumns=True) \
                        if del_count > 0 else clean_df
                    merge_into_silver(spark, merge_df, silver_path, cfg["pk"], cfg.get("partition"))
                    logger.info(
                        f"    ✅ [Batch {batch_id}] {in_count} bronze → "
                        f"{out_count} silver | {q_count} quarantined | {del_count} deleted"
                    )
                else:
                    logger.info(f"    [Batch {batch_id}] 0 records → {cfg['silver']}")
            finally:
                silver_df.unpersist()

        try:
            logger.info(f"  ▶ {cfg['bronze']} → {cfg['silver']} (Incremental)")

            query = (
                spark.readStream.format("delta")
                .option("maxFilesPerTrigger", 100000) # Ép gộp nhiều file nhỏ vào 1 batch
                .option("maxBytesPerTrigger", "500m") # Xử lý tối đa 500MB/batch
                .load(bronze_path)
                .writeStream
                .foreachBatch(process_micro_batch)
                .option("checkpointLocation", chk_dir)
                .trigger(availableNow=True)
                .start()
            )
            query.awaitTermination()
            return True, cfg['silver']

        except Exception as e:
            logger.warning(f"    ⚠️  {cfg['bronze']}: {type(e).__name__}: {e}")
            return False, cfg['silver']

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        for cfg in PIPELINE:
            futures.append(executor.submit(process_cfg, cfg))
            
        for future in as_completed(futures):
            try:
                res_success, table = future.result()
                if res_success is True:
                    success += 1
                elif res_success is False:
                    failed += 1
                else:
                    skipped += 1
            except Exception as e:
                logger.warning(f"⚠️ Worker error: {e}")
                failed += 1

    logger.info(f"═══════ Done: {success} OK | {skipped} skipped | {failed} failed ═══════\n")


if __name__ == "__main__":
    logger.info("🚀 VinaMilk Silver Batch — starting (one-off run, managed by Airflow)")
    logger.info(f"   ADLS Account : {STORAGE_ACCOUNT}")
    logger.info(f"   Bronze path  : {BRONZE_PATH}")
    logger.info(f"   Silver path  : {SILVER_PATH}")

    spark = create_spark()
    try:
        run_silver_batch(spark)
    except Exception as e:
        logger.error(f"❌ Silver batch failed: {e}")
        sys.exit(1)

    logger.info("✅ Silver Batch finished — exiting cleanly for Airflow")

