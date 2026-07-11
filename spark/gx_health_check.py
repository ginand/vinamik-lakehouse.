import os
import sys
import logging
from pyspark.sql import SparkSession

import great_expectations as gx
from great_expectations.data_context.types.base import DataContextConfig, InMemoryStoreBackendDefaults
from great_expectations.core.batch import RuntimeBatchRequest

# ─────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("vinamik.gx_health_check")
logging.getLogger("great_expectations").setLevel(logging.WARNING)

def _env(key, default=""):
    val = os.getenv(key, default)
    return val.strip('"').strip("'") if val else val

STORAGE_ACCOUNT = _env("AZURE_STORAGE_ACCOUNT_NAME", "vmlakehouse2024")
STORAGE_KEY     = _env("AZURE_STORAGE_ACCOUNT_KEY")
SILVER_PATH     = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"

if not STORAGE_KEY:
    logger.error("❌ Thiếu AZURE_STORAGE_ACCOUNT_KEY — kiểm tra .env")
    sys.exit(1)

# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("═══════════════════════════════════════════════")
    logger.info("  VinaMilk Data Quality - Great Expectations")
    logger.info("═══════════════════════════════════════════════")

    # 1. Khởi tạo Spark Session
    spark = (
        SparkSession.builder
        .appName("VinaMilk-GX-HealthCheck")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY)

    # 2. Khởi tạo Great Expectations Context (In-Memory nhưng xuất HTML ra dags/)
    logger.info("🔄 Initializing Great Expectations Context...")
    project_config = DataContextConfig(
        store_backend_defaults=InMemoryStoreBackendDefaults(),
        data_docs_sites={
            "local_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": False,
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "/opt/airflow/dags/gx_data_docs",
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                },
            }
        },
    )
    context = gx.get_context(project_config=project_config)

    # 3. Đọc dữ liệu Silver
    logger.info("📖 Đọc dữ liệu transactions_silver từ ADLS...")
    try:
        df = spark.read.format("delta").load(f"{SILVER_PATH}/transactions_silver")
    except Exception as e:
        logger.error(f"❌ Không thể đọc transactions_silver: {e}")
        sys.exit(0) # Bỏ qua nếu bảng chưa tồn tại

    # Cấu hình Fluent Datasource cho Spark (Hỗ trợ Great Expectations 1.x)
    logger.info("🔌 Khởi tạo Fluent Datasource cho Spark...")
    datasource = context.data_sources.add_spark("spark_datasource")
    data_asset = datasource.add_dataframe_asset(name="transactions_silver")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("my_batch")

    # 5. Các luật kiểm định chất lượng (Expectations)
    logger.info("⚖️ Áp dụng các luật kiểm định (Expectations)...")
    import great_expectations.expectations as gxe
    suite_name = "transactions_silver_suite"
    suite = gx.ExpectationSuite(name=suite_name)
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="txn_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="company_code"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="status", value_set=["POSTED", "DRAFT", "VOID"]))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="total_debit", min_value=0))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="total_credit", min_value=0))
    context.suites.add(suite)

    validation_def = gx.ValidationDefinition(
        name="transactions_validation",
        data=batch_definition,
        suite=suite,
    )
    context.validation_definitions.add(validation_def)

    # 6. Chạy Validation
    logger.info("🚀 Running Validation...")
    results = validation_def.run(batch_parameters={"dataframe": df})

    # 7. Xuất Data Docs (HTML)
    logger.info("📑 Xây dựng Data Docs (HTML)...")
    context.build_data_docs()
    logger.info("✅ Data Docs đã được tạo thành công tại: dags/gx_data_docs/index.html")

    spark.stop()

    if not results.success:
        logger.warning("⚠️ Có bản ghi không thỏa mãn luật kiểm định!")
        sys.exit(1)
    else:
        logger.info("✅ Tất cả dữ liệu đều hợp lệ!")
        sys.exit(0)
