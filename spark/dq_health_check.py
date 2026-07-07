# ─────────────────────────────────────────────────────────
# VinaMilk Data Lakehouse — Data Quality Health Check
# ─────────────────────────────────────────────────────────
# Chạy sau Silver Batch, trước Gold dbt.
# Kiểm tra quarantine tables trên ADLS Gen2:
#   1. Đếm số records lỗi theo bảng và loại lỗi
#   2. Tính error rate (quarantine / total silver records)
#   3. FAIL nếu error rate > 20% (threshold có thể điều chỉnh)
#
# Kết quả ghi ra stdout để Airflow log capture.
# Exit code 0 = OK, 1 = error rate vượt ngưỡng.
# ─────────────────────────────────────────────────────────

import os
import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, current_date

# ─────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("vinamik.dq_health_check")

# ─────────────────────────────────────────────────────────
# ENV CONFIG
# ─────────────────────────────────────────────────────────
def _env(key, default=""):
    val = os.getenv(key, default)
    return val.strip('"').strip("'") if val else val

STORAGE_ACCOUNT   = _env("AZURE_STORAGE_ACCOUNT_NAME", "vmlakehouse2024")
STORAGE_KEY       = _env("AZURE_STORAGE_ACCOUNT_KEY")
ERROR_RATE_THRESHOLD = float(_env("DQ_ERROR_RATE_THRESHOLD", "0.20"))  # 20%

SILVER_PATH       = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
QUARANTINE_PATH   = f"abfss://quarantine@{STORAGE_ACCOUNT}.dfs.core.windows.net"

if not STORAGE_KEY:
    logger.error("❌ Thiếu AZURE_STORAGE_ACCOUNT_KEY — kiểm tra .env")
    sys.exit(1)

# ─────────────────────────────────────────────────────────
# TABLES TO CHECK
# ─────────────────────────────────────────────────────────
TABLES = [
    {"silver": "transactions_silver",    "quarantine": "transactions_silver",    "name": "Transactions"},
    {"silver": "general_ledger_silver",  "quarantine": "general_ledger_silver",  "name": "General Ledger"},
    {"silver": "ar_silver",              "quarantine": "ar_silver",              "name": "Accounts Receivable"},
    {"silver": "ap_silver",              "quarantine": "ap_silver",              "name": "Accounts Payable"},
]

# Error hints for missing tables
_TABLE_NOT_FOUND_HINTS = (
    "Path does not exist",
    "is not a Delta table",
    "doesn't exist",
    "No such file or directory",
    "DELTA_TABLE_NOT_FOUND",
    "DELTA_MISSING_TRANSACTION_LOG",
    "DELTA_SCHEMA_NOT_SET",
    "Table schema is not set",
)


def safe_count(spark, path):
    """Đếm records trong Delta table. Trả về 0 nếu bảng chưa tồn tại."""
    try:
        return spark.read.format("delta").load(path).count()
    except Exception as e:
        if any(hint in str(e) for hint in _TABLE_NOT_FOUND_HINTS):
            return 0
        raise


def count_by_error_type(spark, path):
    """Đếm records theo _error_type. Trả về dict {error_type: count}."""
    try:
        df = spark.read.format("delta").load(path)
        if "_error_type" not in df.columns:
            return {"UNKNOWN": df.count()}
        rows = (
            df.groupBy("_error_type")
            .agg(count("*").alias("cnt"))
            .collect()
        )
        return {row["_error_type"]: row["cnt"] for row in rows}
    except Exception as e:
        if any(hint in str(e) for hint in _TABLE_NOT_FOUND_HINTS):
            return {}
        raise


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("═══════════════════════════════════════════════")
    logger.info("  VinaMilk Data Quality Health Check")
    logger.info(f"  Threshold: {ERROR_RATE_THRESHOLD:.0%}")
    logger.info("═══════════════════════════════════════════════")

    spark = (
        SparkSession.builder
        .appName("VinaMilk-DQ-HealthCheck")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set(
        f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        STORAGE_KEY
    )

    total_silver = 0
    total_quarantine = 0
    has_issues = False

    logger.info("")
    logger.info("┌──────────────────────────┬──────────┬──────────────┬──────────┐")
    logger.info("│ Table                    │  Silver  │  Quarantine  │  Rate %  │")
    logger.info("├──────────────────────────┼──────────┼──────────────┼──────────┤")

    for tbl in TABLES:
        silver_path = f"{SILVER_PATH}/{tbl['silver']}"
        quarantine_path = f"{QUARANTINE_PATH}/{tbl['quarantine']}"

        s_count = safe_count(spark, silver_path)
        q_count = safe_count(spark, quarantine_path)

        total_silver += s_count
        total_quarantine += q_count

        # Tính error rate: quarantine / (silver + quarantine)
        total_records = s_count + q_count
        rate = (q_count / total_records * 100) if total_records > 0 else 0.0

        name_padded = tbl["name"].ljust(24)
        logger.info(
            f"│ {name_padded} │ {s_count:>8,} │ {q_count:>12,} │ {rate:>6.1f}% │"
        )

        # Chi tiết lỗi theo loại
        if q_count > 0:
            error_breakdown = count_by_error_type(spark, quarantine_path)
            for err_type, err_count in sorted(error_breakdown.items(), key=lambda x: -x[1]):
                logger.info(f"│   └─ {err_type:<20} │          │ {err_count:>12,} │          │")

    logger.info("├──────────────────────────┼──────────┼──────────────┼──────────┤")

    grand_total = total_silver + total_quarantine
    overall_rate = (total_quarantine / grand_total * 100) if grand_total > 0 else 0.0

    logger.info(
        f"│ {'TOTAL':24} │ {total_silver:>8,} │ {total_quarantine:>12,} │ {overall_rate:>6.1f}% │"
    )
    logger.info("└──────────────────────────┴──────────┴──────────────┴──────────┘")
    logger.info("")

    # ── Verdict ──
    if grand_total == 0:
        logger.info("ℹ️  Chưa có dữ liệu nào trong Silver/Quarantine — bỏ qua kiểm tra.")
        sys.exit(0)

    if overall_rate > ERROR_RATE_THRESHOLD * 100:
        logger.error(
            f"❌ ERROR RATE {overall_rate:.1f}% VƯỢT NGƯỠNG {ERROR_RATE_THRESHOLD:.0%}! "
            f"({total_quarantine:,} / {grand_total:,} records)"
        )
        logger.error("   Kiểm tra data generator hoặc Bronze data có vấn đề.")
        logger.error("   Xem chi tiết: quarantine container trên ADLS Gen2")
        sys.exit(1)
    else:
        logger.info(
            f"✅ Data Quality OK — Error rate: {overall_rate:.1f}% "
            f"(ngưỡng: {ERROR_RATE_THRESHOLD:.0%})"
        )
        logger.info(
            f"   Silver: {total_silver:,} records sạch | "
            f"Quarantine: {total_quarantine:,} records lỗi"
        )
        sys.exit(0)
