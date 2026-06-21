# ─────────────────────────────────────────────────────────
# VinaMilk Data Lakehouse — Bronze Layer Streaming (Docker)
# ─────────────────────────────────────────────────────────
# Chạy trên Docker container (bitnami/spark).
# Nhiệm vụ: Đọc dữ liệu real-time từ Azure Event Hubs (Kafka protocol)
#            → Ghi xuống Azure Data Lake Storage Gen2 (Delta format).
#
# Đây là service thứ 8 trong docker-compose.yml.
# Tất cả credentials được truyền qua biến môi trường (.env).
# ─────────────────────────────────────────────────────────

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType

# ─────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("vinamik.bronze_streaming")

# ─────────────────────────────────────────────────────────
# 1. ĐỌC BIẾN MÔI TRƯỜNG (từ docker-compose env_file)
# ─────────────────────────────────────────────────────────
def _env(key, default=""):
    """Đọc env var và bỏ dấu ngoặc kép thừa do Docker --env-file giữ lại."""
    val = os.getenv(key, default)
    return val.strip('"').strip("'") if val else val

STORAGE_ACCOUNT   = _env("AZURE_STORAGE_ACCOUNT_NAME", "vmlakehouse2024")
STORAGE_KEY       = _env("AZURE_STORAGE_ACCOUNT_KEY")
EH_SERVER         = _env("EVENT_HUBS_SERVER", "vnm-eventhubs-2024.servicebus.windows.net:9093")
EH_CONN_STRING    = _env("EVENT_HUBS_CONNECTION_STRING")

if not STORAGE_KEY or not EH_CONN_STRING:
    logger.error("❌ Thiếu biến môi trường AZURE_STORAGE_ACCOUNT_KEY hoặc EVENT_HUBS_CONNECTION_STRING")
    logger.error("   Kiểm tra file .env hoặc docker-compose.yml env_file")
    sys.exit(1)

# Đường dẫn ADLS Gen2
BRONZE_PATH     = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
CHECKPOINT_PATH = f"abfss://checkpoints@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# SASL config cho Event Hubs (Kafka protocol)
EH_SASL = (
    'org.apache.kafka.common.security.plain.PlainLoginModule '
    f'required username="$ConnectionString" password="{EH_CONN_STRING}";'
)

# ─────────────────────────────────────────────────────────
# 2. DANH SÁCH CÁC TOPIC CẦN INGEST (Bronze Layer)
# ─────────────────────────────────────────────────────────
# Mỗi topic từ Event Hubs sẽ được ghi thành 1 bảng Delta riêng
TOPICS = [
    {"topic": "erp.misa_invoices",       "table": "misa_invoices_bronze"},
    {"topic": "erp.transactions",        "table": "transactions_bronze"},
    {"topic": "erp.general_ledger",      "table": "general_ledger_bronze"},
    {"topic": "erp.accounts_receivable", "table": "ar_bronze"},
    {"topic": "erp.accounts_payable",    "table": "ap_bronze"},
    {"topic": "erp.customers",           "table": "customers_bronze"},
    {"topic": "erp.vendors",             "table": "vendors_bronze"},
    {"topic": "erp.cost_centers",        "table": "cost_centers_bronze"},
    {"topic": "erp.fx_rates",            "table": "fx_rates_bronze"},
    {"topic": "erp.budget_plan",         "table": "budget_plan_bronze"},
]

# ─────────────────────────────────────────────────────────
# 3. KHỞI TẠO SPARK SESSION
# ─────────────────────────────────────────────────────────
logger.info("🚀 Khởi tạo SparkSession...")
# Các packages (Delta, Kafka, hadoop-azure) được truyền qua spark-submit --packages
# nên chỉ cần getOrCreate() thuần tuý ở đây
spark = (
    SparkSession.builder
    .appName("VinaMilk-Bronze-Streaming")
    .config("spark.driver.memory", "1g")
    .config("spark.executor.memory", "1g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Cấu hình xác thực ADLS Gen2
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

logger.info("✅ SparkSession đã sẵn sàng")

# ─────────────────────────────────────────────────────────
# 4. HÀM TẠO STREAMING QUERY CHO MỖI TOPIC
# ─────────────────────────────────────────────────────────
def create_bronze_stream(topic_name: str, table_name: str):
    """
    Tạo một Structured Streaming query:
      - Đọc từ Azure Event Hubs (Kafka protocol)
      - Giữ nguyên dữ liệu thô (raw JSON) + metadata
      - Ghi xuống ADLS Gen2 dưới định dạng Delta
    """
    logger.info(f"📡 Đang kết nối topic: {topic_name} → {table_name}")

    # Đọc stream từ Kafka/Event Hubs
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", EH_SERVER)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.jaas.config", EH_SASL)
        .option("kafka.request.timeout.ms", "60000")
        .option("kafka.session.timeout.ms", "30000")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Bronze transformation: giữ nguyên raw, chỉ thêm metadata
    bronze_df = (
        raw_df
        .withColumn("kafka_key", col("key").cast(StringType()))
        .withColumn("raw_payload", col("value").cast(StringType()))
        .withColumn("kafka_partition", col("partition"))
        .withColumn("kafka_offset", col("offset"))
        .withColumn("kafka_timestamp", col("timestamp"))
        .withColumn("ingested_at", current_timestamp())
        .select(
            "kafka_key", "raw_payload",
            "kafka_partition", "kafka_offset",
            "kafka_timestamp", "ingested_at"
        )
    )

    # Ghi xuống Delta Lake
    target_dir = f"{BRONZE_PATH}/{table_name}"
    chk_dir = f"{CHECKPOINT_PATH}/bronze_{table_name}"

    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", chk_dir)
        .option("path", target_dir)
        .trigger(processingTime="30 seconds")
        .queryName(f"bronze_{table_name}")
        .start()
    )

    logger.info(f"✅ Streaming {topic_name} → {target_dir}")
    return query


# ─────────────────────────────────────────────────────────
# 5. KHỞI CHẠY TẤT CẢ STREAMING QUERIES
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info(f"═══════════════════════════════════════════════")
    logger.info(f"  VinaMilk Bronze Streaming — {len(TOPICS)} topics")
    logger.info(f"  Event Hubs: {EH_SERVER}")
    logger.info(f"  Storage:    {STORAGE_ACCOUNT}")
    logger.info(f"═══════════════════════════════════════════════")

    active_queries = []
    for t in TOPICS:
        try:
            q = create_bronze_stream(t["topic"], t["table"])
            active_queries.append(q)
        except Exception as e:
            logger.warning(f"⚠️  Không thể kết nối topic {t['topic']}: {e}")
            logger.warning(f"   (Topic có thể chưa có dữ liệu — sẽ bỏ qua)")

    if not active_queries:
        logger.error("❌ Không có streaming query nào chạy được. Kiểm tra Event Hubs connection.")
        sys.exit(1)

    logger.info(f"🎯 Đang chạy {len(active_queries)}/{len(TOPICS)} streaming queries")
    logger.info(f"   Nhấn Ctrl+C để dừng tất cả.")

    # Monitoring loop: topic nào crash thì bỏ qua, không kéo chết cả hệ thống
    import time
    while active_queries:
        time.sleep(15)
        still_running = []
        for q in active_queries:
            try:
                if q.isActive:
                    still_running.append(q)
                else:
                    exc = q.exception()
                    if exc:
                        logger.warning(f"⚠️  Query '{q.name}' dừng do lỗi: {type(exc).__name__}")
                        logger.warning(f"   (Bỏ qua và tiếp tục với các queries còn lại)")
                    else:
                        logger.info(f"ℹ️  Query '{q.name}' dừng bình thường.")
            except Exception as e:
                logger.warning(f"⚠️  Không thể kiểm tra trạng thái query '{q.name}': {e}")

        active_queries = still_running
        if active_queries:
            logger.info(f"   [{len(active_queries)} queries đang chạy] ...")

    logger.info("🏁 Tất cả streaming queries đã kết thúc.")
