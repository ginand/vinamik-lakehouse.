# ─────────────────────────────────────────────────────────
# VinaMilk Data Lakehouse — Gold Layer Batch Processing
# ─────────────────────────────────────────────────────────
# Silver (clean data) → Gold (business KPIs, aggregations)
#
# Chạy định kỳ mỗi RUN_INTERVAL_MINUTES phút (mặc định: 15).
#
# KPI tables được tạo:
#   1. revenue_by_product_gold      — Doanh thu theo dòng sản phẩm
#   2. ar_aging_gold                — Phân tích công nợ phải thu
#   3. ap_aging_gold                — Phân tích công nợ phải trả
#   4. budget_vs_actual_gold        — Ngân sách vs Thực tế
#   5. gl_trial_balance_gold        — Cân đối số phát sinh (bảng cân đối kế toán sơ bộ)
#   6. cash_flow_summary_gold       — Tóm tắt dòng tiền
# ─────────────────────────────────────────────────────────

import os
import sys
import time
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as _sum, count, avg, max as _max, min as _min,
    when, lit, round as _round, current_timestamp, coalesce,
    to_date, date_format, year, month, expr
)

# ─────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("vinamik.gold_batch")

# ─────────────────────────────────────────────────────────
# ENV CONFIG
# ─────────────────────────────────────────────────────────
def _env(key, default=""):
    val = os.getenv(key, default)
    return val.strip('"').strip("'") if val else val

STORAGE_ACCOUNT   = _env("AZURE_STORAGE_ACCOUNT_NAME", "vmlakehouse2024")
STORAGE_KEY       = _env("AZURE_STORAGE_ACCOUNT_KEY")
RUN_INTERVAL_MIN  = int(_env("RUN_INTERVAL_MINUTES", "15"))

SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_PATH   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

if not STORAGE_KEY:
    logger.error("❌ Thiếu AZURE_STORAGE_ACCOUNT_KEY — kiểm tra .env")
    sys.exit(1)

# ─────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────
def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("VinaMilk-Gold-Batch")
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


def read_silver(spark: SparkSession, table: str) -> DataFrame:
    """Đọc Silver Delta table, chỉ lấy records sạch (không bị delete, không có DQ lỗi)."""
    path = f"{SILVER_PATH}/{table}"
    df = spark.read.format("delta").load(path)
    if "_is_deleted" in df.columns:
        df = df.filter(col("_is_deleted") == False)  # noqa: E712
    return df


def write_gold(df: DataFrame, table: str, mode: str = "overwrite"):
    """Ghi Gold Delta table — overwrite toàn bộ (full recompute mỗi lần chạy)."""
    path = f"{GOLD_PATH}/{table}"
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(path)
    )


# ─────────────────────────────────────────────────────────
# GOLD KPI 1: DOANH THU THEO DÒNG SẢN PHẨM
# ─────────────────────────────────────────────────────────
# GL accounts 511x = Revenue:
#   5111 = Sữa tươi UHT
#   5112 = Sữa đặc
#   5113 = Sữa bột baby
#   5114 = Sữa chua
#   5115 = Kem & Nước trái cây
# ─────────────────────────────────────────────────────────
PRODUCT_ACCOUNT_MAP = {
    "5111": "Sữa tươi UHT",
    "5112": "Sữa đặc Ông Thọ / Ngôi Sao",
    "5113": "Sữa bột Dielac",
    "5114": "Sữa chua / ProYogurt",
    "5115": "Kem & Nước trái cây Vfresh",
}


def build_revenue_by_product(spark: SparkSession) -> DataFrame:
    """
    Doanh thu theo:
      - product_line (GL account 511x)
      - fiscal_year, fiscal_period, posting_month
      - company_code, cost_center
    """
    gl = read_silver(spark, "general_ledger_silver")
    txn = read_silver(spark, "transactions_silver")

    # Chỉ lấy Credit lines trên tài khoản doanh thu (511x)
    revenue_gl = (
        gl
        .filter(col("account_id").startswith("511"))
        .filter(col("debit_credit") == "C")         # Revenue là Credit
        .filter(col("dq_is_clean") == True)          # noqa: E712
        .filter(col("amount_vnd") > 0)
    )

    # Join với transactions để lấy fiscal info
    result = (
        revenue_gl
        .join(
            txn.select("txn_id", "company_code", "fiscal_year", "fiscal_period",
                       "posting_date", "posting_month", "currency", "status"),
            on="txn_id",
            how="left"
        )
        .filter(col("status").isin("POSTED"))
        .withColumn("product_line", col("account_id").substr(1, 4))
        .withColumn("product_name",
            when(col("product_line") == "5111", lit("Sữa tươi UHT"))
            .when(col("product_line") == "5112", lit("Sữa đặc Ông Thọ / Ngôi Sao"))
            .when(col("product_line") == "5113", lit("Sữa bột Dielac"))
            .when(col("product_line") == "5114", lit("Sữa chua / ProYogurt"))
            .when(col("product_line") == "5115", lit("Kem & Nước trái cây"))
            .otherwise(lit("Khác"))
        )
        .groupBy(
            "product_line", "product_name",
            "company_code", "cost_center",
            "fiscal_year", "fiscal_period", "posting_month"
        )
        .agg(
            _round(_sum("amount_vnd"), 0).alias("revenue_vnd"),
            count("gl_id").alias("num_line_items"),
            count("txn_id").alias("num_transactions"),
            _round(avg("amount_vnd"), 0).alias("avg_revenue_per_line"),
        )
        .withColumn("_gold_computed_at", current_timestamp())
    )
    return result


# ─────────────────────────────────────────────────────────
# GOLD KPI 2: PHÂN TÍCH CÔNG NỢ PHẢI THU (AR AGING)
# ─────────────────────────────────────────────────────────
def build_ar_aging(spark: SparkSession) -> DataFrame:
    """
    Tổng hợp AR theo:
      - customer_id, sales_channel, aging_bucket
      - Tính tổng outstanding, số lượng invoice
    """
    ar = read_silver(spark, "ar_silver")
    customers = read_silver(spark, "customers_silver")

    result = (
        ar
        .filter(~col("status").isin("PAID"))          # Chỉ open items
        .join(
            customers.select("customer_id", "customer_name", "customer_type",
                             "province", "sales_region", "credit_limit"),
            on="customer_id",
            how="left"
        )
        .groupBy(
            "customer_id", "customer_name", "customer_type",
            "sales_channel", "sales_region", "province",
            "aging_bucket", "invoice_month"
        )
        .agg(
            count("ar_id").alias("num_invoices"),
            _round(_sum("outstanding_vnd"), 0).alias("total_outstanding_vnd"),
            _round(_sum("amount_vnd"), 0).alias("total_invoiced_vnd"),
            _round(_sum("paid_amount"), 0).alias("total_paid_vnd"),
            _round(avg("overdue_days"), 1).alias("avg_overdue_days"),
            _round(_max("overdue_days"), 0).alias("max_overdue_days"),
            _round(_max("credit_limit"), 0).alias("credit_limit"),
        )
        .withColumn("collection_rate_pct",
            _round(col("total_paid_vnd") / col("total_invoiced_vnd") * 100, 2)
        )
        .withColumn("_gold_computed_at", current_timestamp())
    )
    return result


# ─────────────────────────────────────────────────────────
# GOLD KPI 3: PHÂN TÍCH CÔNG NỢ PHẢI TRẢ (AP AGING)
# ─────────────────────────────────────────────────────────
def build_ap_aging(spark: SparkSession) -> DataFrame:
    """
    Tổng hợp AP theo:
      - vendor_id, vendor_type, aging_bucket
    """
    ap = read_silver(spark, "ap_silver")
    vendors = read_silver(spark, "vendors_silver")

    result = (
        ap
        .filter(~col("status").isin("PAID"))
        .join(
            vendors.select("vendor_id", "vendor_name", "vendor_type", "country"),
            on="vendor_id",
            how="left"
        )
        .groupBy(
            "vendor_id", "vendor_name", "vendor_type",
            "country", "aging_bucket", "invoice_month"
        )
        .agg(
            count("ap_id").alias("num_invoices"),
            _round(_sum("outstanding_vnd"), 0).alias("total_outstanding_vnd"),
            _round(_sum("amount_vnd"), 0).alias("total_invoiced_vnd"),
            _round(_sum("paid_amount"), 0).alias("total_paid_vnd"),
            _round(avg("overdue_days"), 1).alias("avg_overdue_days"),
            _round(_max("overdue_days"), 0).alias("max_overdue_days"),
        )
        .withColumn("payment_rate_pct",
            _round(col("total_paid_vnd") / col("total_invoiced_vnd") * 100, 2)
        )
        .withColumn("_gold_computed_at", current_timestamp())
    )
    return result


# ─────────────────────────────────────────────────────────
# GOLD KPI 4: NGÂN SÁCH VS THỰC TẾ (BUDGET vs ACTUAL)
# ─────────────────────────────────────────────────────────
BUDGET_PAYLOAD_SCHEMA_STR = "`cost_center` STRING, `gl_account` STRING, `fiscal_year` INT, `fiscal_period` INT, `budgeted_amount_vnd` DOUBLE, `budget_version` STRING"


def build_budget_vs_actual(spark: SparkSession) -> DataFrame:
    """
    So sánh kế hoạch ngân sách (budget_plan_silver) vs
    chi phí thực tế (general_ledger_silver — debit lines trên tài khoản chi phí 6xx, 7xx)
    """
    from pyspark.sql.functions import from_json
    from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType

    # ── Budget từ budget_plan topic (flat JSON từ custom producer) ──
    budget_schema = StructType([
        StructField("cost_center",         StringType()),
        StructField("gl_account",          StringType()),
        StructField("fiscal_year",         IntegerType()),
        StructField("fiscal_period",       IntegerType()),
        StructField("budgeted_amount_vnd", DoubleType()),
        StructField("budget_version",      StringType()),
    ])

    try:
        budget_raw = spark.read.format("delta").load(f"{SILVER_PATH}/budget_plan_silver")
        budget = (
            budget_raw
            .withColumn("_b", from_json(col("raw_payload"), budget_schema))
            .select(
                col("_b.cost_center").alias("cost_center"),
                col("_b.gl_account").alias("gl_account"),
                col("_b.fiscal_year").alias("fiscal_year"),
                col("_b.fiscal_period").alias("fiscal_period"),
                col("_b.budgeted_amount_vnd").alias("budgeted_amount_vnd"),
            )
            .filter(col("cost_center").isNotNull())
            .groupBy("cost_center", "gl_account", "fiscal_year", "fiscal_period")
            .agg(_round(_sum("budgeted_amount_vnd"), 0).alias("budgeted_amount_vnd"))
        )
    except Exception:
        # Budget table not available yet — return empty
        logger.warning("   ⚠️  budget_plan_silver chưa có data — bỏ qua budget vs actual")
        return spark.createDataFrame([], schema="cost_center STRING, message STRING")

    # ── Actual từ GL (debit lines trên accounts chi phí 6xx/7xx) ──
    gl = read_silver(spark, "general_ledger_silver")
    txn = read_silver(spark, "transactions_silver")

    actual = (
        gl
        .filter(col("account_id").rlike("^[67]"))    # 6xx = chi phí, 7xx = giá vốn
        .filter(col("debit_credit") == "D")
        .filter(col("dq_is_clean") == True)           # noqa: E712
        .join(
            txn.select("txn_id", "fiscal_year", "fiscal_period", "status"),
            on="txn_id", how="left"
        )
        .filter(col("status").isin("POSTED"))
        .groupBy("cost_center", "account_id", "fiscal_year", "fiscal_period")
        .agg(_round(_sum("amount_vnd"), 0).alias("actual_amount_vnd"))
        .withColumnRenamed("account_id", "gl_account")
    )

    # ── JOIN budget vs actual ──
    result = (
        budget
        .join(actual, on=["cost_center", "gl_account", "fiscal_year", "fiscal_period"], how="outer")
        .withColumn("budgeted_amount_vnd",  coalesce(col("budgeted_amount_vnd"), lit(0.0)))
        .withColumn("actual_amount_vnd",    coalesce(col("actual_amount_vnd"), lit(0.0)))
        .withColumn("variance_vnd",
            col("actual_amount_vnd") - col("budgeted_amount_vnd"))
        .withColumn("achievement_pct",
            when(col("budgeted_amount_vnd") != 0,
                _round(col("actual_amount_vnd") / col("budgeted_amount_vnd") * 100, 2)
            ).otherwise(lit(None))
        )
        .withColumn("status_flag",
            when(col("actual_amount_vnd") > col("budgeted_amount_vnd") * 1.1, lit("OVER_BUDGET"))
            .when(col("actual_amount_vnd") < col("budgeted_amount_vnd") * 0.9, lit("UNDER_BUDGET"))
            .otherwise(lit("ON_TRACK"))
        )
        .withColumn("_gold_computed_at", current_timestamp())
    )
    return result


# ─────────────────────────────────────────────────────────
# GOLD KPI 5: BẢNG CÂN ĐỐI SỐ PHÁT SINH (GL TRIAL BALANCE)
# ─────────────────────────────────────────────────────────
def build_trial_balance(spark: SparkSession) -> DataFrame:
    """
    Bảng cân đối số phát sinh theo:
      - account_id, company_code, fiscal_year, fiscal_period
      - Tổng Nợ / Có (Debit / Credit)
    """
    gl = read_silver(spark, "general_ledger_silver")
    txn = read_silver(spark, "transactions_silver")

    result = (
        gl
        .filter(col("dq_is_clean") == True)  # noqa: E712
        .join(
            txn.select("txn_id", "company_code", "fiscal_year",
                       "fiscal_period", "posting_month", "status"),
            on="txn_id", how="left"
        )
        .filter(col("status").isin("POSTED"))
        .groupBy("account_id", "company_code", "fiscal_year", "fiscal_period", "posting_month")
        .agg(
            _round(_sum(when(col("debit_credit") == "D", col("amount_vnd")).otherwise(0)), 0)
            .alias("total_debit_vnd"),
            _round(_sum(when(col("debit_credit") == "C", col("amount_vnd")).otherwise(0)), 0)
            .alias("total_credit_vnd"),
            count("gl_id").alias("num_line_items"),
        )
        .withColumn("net_balance_vnd", col("total_debit_vnd") - col("total_credit_vnd"))
        .withColumn("_gold_computed_at", current_timestamp())
    )
    return result


# ─────────────────────────────────────────────────────────
# GOLD KPI 6: TÓM TẮT DÒNG TIỀN (CASH FLOW SUMMARY)
# ─────────────────────────────────────────────────────────
def build_cash_flow_summary(spark: SparkSession) -> DataFrame:
    """
    Tóm tắt dòng tiền theo tháng:
      - Hoạt động kinh doanh: AR collections (DZ) và AP payments (KZ)
      - Hoạt động đầu tư: goods movements (WA)
      - Từ transactions header (doc_type + total_debit/credit)
    """
    txn = read_silver(spark, "transactions_silver")

    result = (
        txn
        .filter(col("status").isin("POSTED"))
        .withColumn("flow_category",
            when(col("doc_type").isin("DZ"), lit("INFLOW_AR_COLLECTION"))
            .when(col("doc_type").isin("KZ"), lit("OUTFLOW_AP_PAYMENT"))
            .when(col("doc_type").isin("RV", "DR"), lit("REVENUE_INVOICE"))
            .when(col("doc_type").isin("KR", "RE"), lit("PURCHASE_INVOICE"))
            .when(col("doc_type").isin("SA"), lit("GENERAL_POSTING"))
            .when(col("doc_type").isin("WA"), lit("GOODS_MOVEMENT"))
            .otherwise(lit("OTHER"))
        )
        .groupBy("company_code", "fiscal_year", "fiscal_period", "posting_month", "flow_category")
        .agg(
            count("txn_id").alias("num_transactions"),
            _round(_sum("total_debit"),  0).alias("total_debit_vnd"),
            _round(_sum("total_credit"), 0).alias("total_credit_vnd"),
        )
        .withColumn("net_flow_vnd", col("total_credit_vnd") - col("total_debit_vnd"))
        .withColumn("_gold_computed_at", current_timestamp())
    )
    return result


# ─────────────────────────────────────────────────────────
# GOLD PIPELINE RUNNER
# ─────────────────────────────────────────────────────────
GOLD_PIPELINE = [
    {
        "name":       "Doanh thu theo sản phẩm",
        "table":      "revenue_by_product_gold",
        "build_fn":   build_revenue_by_product,
    },
    {
        "name":       "AR Aging phân tích công nợ phải thu",
        "table":      "ar_aging_gold",
        "build_fn":   build_ar_aging,
    },
    {
        "name":       "AP Aging phân tích công nợ phải trả",
        "table":      "ap_aging_gold",
        "build_fn":   build_ap_aging,
    },
    {
        "name":       "Ngân sách vs Thực tế",
        "table":      "budget_vs_actual_gold",
        "build_fn":   build_budget_vs_actual,
    },
    {
        "name":       "Bảng cân đối số phát sinh",
        "table":      "gl_trial_balance_gold",
        "build_fn":   build_trial_balance,
    },
    {
        "name":       "Tóm tắt dòng tiền",
        "table":      "cash_flow_summary_gold",
        "build_fn":   build_cash_flow_summary,
    },
]


def run_gold_batch(spark: SparkSession):
    run_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info(f"═══════ Gold Batch Run @ {run_ts} ═══════")
    success, failed = 0, 0

    for cfg in GOLD_PIPELINE:
        try:
            logger.info(f"  ▶ {cfg['name']}")
            df = cfg["build_fn"](spark)

            if df.rdd.isEmpty():
                logger.info(f"    ℹ️  Silver không có đủ data — bỏ qua")
                continue

            write_gold(df, cfg["table"])
            row_count = df.count()
            logger.info(f"    ✅ Đã ghi {row_count} rows → gold/{cfg['table']}")
            success += 1
        except Exception as e:
            logger.warning(f"    ⚠️  {cfg['table']}: {type(e).__name__}: {e}")
            failed += 1

    logger.info(f"═══════ Done: {success} OK | {failed} failed ═══════\n")


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("🏆 VinaMilk Gold Batch — starting")
    logger.info(f"   ADLS Account  : {STORAGE_ACCOUNT}")
    logger.info(f"   Run interval  : every {RUN_INTERVAL_MIN} minutes")
    logger.info(f"   Silver path   : {SILVER_PATH}")
    logger.info(f"   Gold path     : {GOLD_PATH}")

    spark = create_spark()

    # Đợi Silver sẵn sàng (lần đầu)
    logger.info("⏳ Đợi 2 phút để Silver batch chạy trước...")
    time.sleep(120)

    while True:
        try:
            run_gold_batch(spark)
        except Exception as e:
            logger.error(f"❌ Gold batch crashed: {e}")
        logger.info(f"💤 Sleeping {RUN_INTERVAL_MIN} minutes until next run...")
        time.sleep(RUN_INTERVAL_MIN * 60)
