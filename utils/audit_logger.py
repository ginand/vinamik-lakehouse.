# ─────────────────────────────────────────────────────────
# utils/audit_logger.py
# Ghi log lịch sử truy cập (Audit Logging) trên Gold Layer
# Tham chiếu: Mục 11.3 - Đề tài Tốt nghiệp
# ─────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from dataclasses import dataclass
from datetime import datetime

AUDIT_PATH = "abfss://gold@vmlakehouse.dfs.core.windows.net/audit_log"
# Dành cho chạy local docker:
LOCAL_AUDIT_PATH = "/tmp/lakehouse/audit_log"

@dataclass
class AuditEvent:
    user_id:     str
    action:      str        # READ / WRITE / DELETE
    table_name:  str
    row_count:   int
    query_hash:  str = ""


def log_audit_event(spark: SparkSession, event: AuditEvent):
    """
    Ghi nhận thao tác truy cập vào bảng audit_log để phục vụ kiểm toán nội bộ.
    """
    record = [{
        "user_id":     event.user_id,
        "action":      event.action,
        "table_name":  event.table_name,
        "row_count":   event.row_count,
        "timestamp":   datetime.utcnow().isoformat(),
        "query_hash":  event.query_hash,
        "environment": "production",
    }]
    
    # Ưu tiên ghi vào ADLS nếu có, không thì ghi local
    target_path = AUDIT_PATH if spark.conf.get("fs.azure.account.key.vmlakehouse.dfs.core.windows.net", None) else LOCAL_AUDIT_PATH
    
    try:
        (
            spark.createDataFrame(record)
            .write.format("delta")
            .mode("append")
            .save(target_path)
        )
    except Exception as e:
        print(f"[WARNING] Không thể ghi audit log: {e}")

# Cách dùng:
# log_audit_event(spark, AuditEvent(user_id="BI_SERVICE", action="READ", table_name="gold_revenue_daily", row_count=1500))
