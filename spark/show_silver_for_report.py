import os
from pyspark.sql import SparkSession

# Lấy biến môi trường Azure
STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "vmlakehouse2024")
STORAGE_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# Khởi tạo Spark Session
spark = (
    SparkSession.builder
    .appName("Show-Silver-For-Report")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

if STORAGE_KEY:
    spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY)

print("\n" + "="*80)
print("📸 KẾT QUẢ TRUY VẤN BẢNG TRANSACTIONS_SILVER (ĐỂ CHỤP ẢNH BÁO CÁO)")
print("="*80 + "\n")

try:
    # Đọc bảng Delta transactions_silver
    df_silver = spark.read.format("delta").load(f"{SILVER_PATH}/transactions_silver")
    
    # Lọc ra các cột quan trọng và các cờ Data Quality để show lên ảnh cho đẹp
    df_show = df_silver.select(
        "txn_id", 
        "total_debit", 
        "currency", 
        "status",
        "dq_amount_zero", 
        "dq_is_clean"
    )
    
    # Hiển thị 20 dòng
    df_show.show(20, truncate=False)
except Exception as e:
    print(f"Lỗi khi đọc bảng: {e}")

print("="*80)
print("💡 Mẹo: Bạn có thể thay 'transactions_silver' thành 'quarantine/transactions_silver' để chụp các bản ghi lỗi!")
print("="*80 + "\n")
