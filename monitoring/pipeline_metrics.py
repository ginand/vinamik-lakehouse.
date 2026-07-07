# ─────────────────────────────────────────────────────────
# monitoring/pipeline_metrics.py
# Đo thời gian chạy và số dòng xử lý của các Pipeline Jobs
# Tham chiếu: Mục 12.3 - Đề tài Tốt nghiệp
# ─────────────────────────────────────────────────────────

import time
from functools import wraps
from datetime import datetime
from pyspark.sql import SparkSession

# Dành cho chạy local docker:
LOCAL_METRICS_PATH = "/tmp/lakehouse/job_metrics"
ADLS_METRICS_PATH  = "abfss://monitor@vmlakehouse.dfs.core.windows.net/job_metrics"

def track_job(job_name: str, spark_instance: SparkSession = None):
    """
    Decorator đo thời gian chạy (duration_s) và số record (row_count)
    của mỗi Spark job.
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            
            # Chạy hàm gốc
            result = fn(*args, **kwargs)
            
            # Tính duration
            duration_s = round(time.time() - start_time, 2)
            
            # Tính số dòng (nếu hàm trả về DataFrame)
            row_count = result.count() if hasattr(result, "count") else -1
            
            # Ghi metrics
            metric = [{
                "job_name":   job_name,
                "run_date":   datetime.utcnow().date().isoformat(),
                "duration_s": duration_s,
                "row_count":  row_count,
                "status":     "SUCCESS",
            }]
            
            # Lấy spark instance từ tham số truyền vào decorator, hoặc tham số đầu tiên của hàm gốc
            spark = spark_instance
            if not spark and args and isinstance(args[0], SparkSession):
                spark = args[0]
                
            if spark:
                target_path = ADLS_METRICS_PATH if spark.conf.get("fs.azure.account.key.vmlakehouse.dfs.core.windows.net", None) else LOCAL_METRICS_PATH
                try:
                    (
                        spark.createDataFrame(metric)
                        .write.format("delta")
                        .mode("append")
                        .save(target_path)
                    )
                except Exception as e:
                    print(f"[WARNING] Không thể ghi metrics: {e}")
                    
            print(f"[METRICS] {job_name}: {duration_s}s, {row_count} rows")
            
            return result
        return wrapper
    return decorator

# Cách dùng:
# @track_job("silver_transactions_batch")
# def run_silver_pipeline(date): ...
