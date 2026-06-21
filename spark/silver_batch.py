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

# ─────────────────────────────────────────────────────────
# ENV CONFIG
# ─────────────────────────────────────────────────────────
def _env(key, default=""):
    val = os.getenv(key, default)
    return val.strip('"').strip("'") if val else val

STORAGE_ACCOUNT   = _env("AZURE_STORAGE_ACCOUNT_NAME", "vmlakehouse2024")
STORAGE_KEY       = _env("AZURE_STORAGE_ACCOUNT_KEY")
RUN_INTERVAL_MIN  = int(_env("RUN_INTERVAL_MINUTES", "5"))

BRONZE_PATH       = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_PATH       = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
CHECKPOINT_PATH   = f"abfss://checkpoints@{STORAGE_ACCOUNT}.dfs.core.windows.net"

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
    StructField("currency",       StringType()),
    StructField("rate_vnd",       DoubleType()),
    StructField("source",         StringType()),
    StructField("effective_date", StringType()),
    StructField("fetched_at",     StringType()),
])

# ─────────────────────────────────────────────────────────
# HELPER: Parse Debezium CDC envelope
# ─────────────────────────────────────────────────────────
def parse_cdc(df: DataFrame, payload_schema: StructType, after_col: str = "after") -> DataFrame:
    """
    Debezium CDC envelope (JSON):
      { "op": "c|u|d|r", "ts_ms": 1234, "before": {...}, "after": {...} }

    - op=c (create/insert): after = new row
    - op=u (update):        after = updated row
    - op=r (snapshot read): after = existing row (initial snapshot)
    - op=d (delete):        before = row being deleted, after = null

    Returns a DataFrame with columns:
      _cdc_op, _cdc_ts_ms, _is_deleted, _silver_loaded_at + all payload fields
    """
    outer_schema = StructType([
        StructField("op",     StringType()),
        StructField("ts_ms",  LongType()),
        StructField("before", payload_schema),
        StructField("after",  payload_schema),
    ])

    return (
        df
        .withColumn("_cdc", from_json(col("raw_payload"), outer_schema))
        .withColumn("_cdc_op",     col("_cdc.op"))
        .withColumn("_cdc_ts_ms",  col("_cdc.ts_ms"))
        .withColumn("_is_deleted", col("_cdc.op") == "d")
        # For deletes use "before", otherwise use "after"
        .withColumn("_payload",
            when(col("_cdc.op") == "d", col("_cdc.before"))
            .otherwise(col("_cdc.after"))
        )
        .filter(col("_payload").isNotNull())   # Skip malformed records
        .select("_cdc_op", "_cdc_ts_ms", "_is_deleted", "_payload")
        .select("_cdc_op", "_cdc_ts_ms", "_is_deleted", "_payload.*")
        .withColumn("_silver_loaded_at", current_timestamp())
    )


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
        .withColumn("dq_is_clean",
            ~col("dq_future_posting") & ~col("dq_amount_zero") & ~col("dq_wrong_currency"))
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
        .withColumn("invoice_month", col("invoice_date").cast(StringType()).substr(1, 7))
    )


def transform_customers(df: DataFrame) -> DataFrame:
    parsed = parse_cdc(df, CUSTOMER_PAYLOAD_SCHEMA)
    return parsed.withColumn("created_at", epoch_micros_to_ts("created_at"))


def transform_vendors(df: DataFrame) -> DataFrame:
    parsed = parse_cdc(df, VENDOR_PAYLOAD_SCHEMA)
    return parsed.withColumn("created_at", epoch_micros_to_ts("created_at"))


def transform_fx_rates(df: DataFrame) -> DataFrame:
    """FX rates come from custom producer (flat JSON, no CDC envelope)."""
    return (
        df
        .withColumn("_fx", from_json(col("raw_payload"), FX_PAYLOAD_SCHEMA))
        .select(
            col("_fx.currency").alias("currency"),
            col("_fx.rate_vnd").alias("rate_vnd"),
            col("_fx.source").alias("source"),
            col("_fx.effective_date").alias("effective_date"),
            col("_fx.fetched_at").alias("fetched_at"),
            current_timestamp().alias("_silver_loaded_at"),
        )
        .filter(col("currency").isNotNull() & col("rate_vnd").isNotNull())
    )


# ─────────────────────────────────────────────────────────
# MERGE INTO SILVER (UPSERT for CDC correctness)
# ─────────────────────────────────────────────────────────
def merge_into_silver(
    spark: SparkSession,
    new_df: DataFrame,
    silver_table_path: str,
    pk_col: str,
):
    """
    MERGE new_df into Silver Delta table at silver_table_path.
    Handles INSERT, UPDATE, and soft-DELETE via _is_deleted flag.
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
    except Exception:
        # Table does not exist yet — create it
        (
            new_df.write
            .format("delta")
            .mode("overwrite")
            .save(silver_table_path)
        )


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
]


# ─────────────────────────────────────────────────────────
# MAIN BATCH LOOP
# ─────────────────────────────────────────────────────────
def run_silver_batch(spark: SparkSession):
    run_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info(f"═══════ Silver Batch Run @ {run_ts} ═══════")
    success, skipped, failed = 0, 0, 0

    for cfg in PIPELINE:
        bronze_path = f"{BRONZE_PATH}/{cfg['bronze']}"
        silver_path = f"{SILVER_PATH}/{cfg['silver']}"

        try:
            logger.info(f"  ▶ {cfg['bronze']} → {cfg['silver']}")

            bronze_df = spark.read.format("delta").load(bronze_path)
            if bronze_df.rdd.isEmpty():
                logger.info(f"    ℹ️  Bronze table empty — skip")
                skipped += 1
                continue

            silver_df = cfg["transform"](bronze_df)

            # Remove deleted records from Silver for physical cleanup (optional)
            clean_df = silver_df.filter(col("_is_deleted") == False)  # noqa: E712
            deleted_df = silver_df.filter(col("_is_deleted") == True)  # noqa: E712

            merge_into_silver(spark, silver_df, silver_path, cfg["pk"])

            in_count  = bronze_df.count()
            out_count = clean_df.count()
            del_count = deleted_df.count()
            logger.info(f"    ✅ {in_count} bronze → {out_count} silver (deleted: {del_count})")
            success += 1

        except Exception as e:
            logger.warning(f"    ⚠️  {cfg['bronze']}: {type(e).__name__}: {e}")
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
