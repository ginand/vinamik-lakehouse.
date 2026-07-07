# ─────────────────────────────────────────────────────────
# monitoring/freshness_check.py
# Kiểm tra Data Freshness SLA cho Lakehouse
# Tham chiếu: Mục 12.2 - Đề tài Tốt nghiệp
# ─────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql.functions import max as spark_max
from datetime import datetime
import sys

SLA_MINUTES = {
    "bronze/transactions":     5,
    "silver/transactions":     20,
    "gold/gold_revenue_daily": 35,
    "gold/gold_ar_aging":      35,
}

def check_freshness(spark: SparkSession = None):
    """
    Kiểm tra xem dữ liệu có được nạp đúng SLA không.
    Trả về True nếu tất cả đều đạt SLA, False nếu có vi phạm.
    """
    if not spark:
        spark = SparkSession.builder.appName("Data_Freshness_Check").getOrCreate()
        
    violations = []
    
    for table, sla_min in SLA_MINUTES.items():
        layer, name = table.split("/")
        
        # Mặc định trỏ về local (dùng để test), nếu chạy trên cloud thì sửa thành abfss
        path = f"/tmp/lakehouse/{layer}/erp/{name}"
        if spark.conf.get("fs.azure.account.key.vmlakehouse.dfs.core.windows.net", None):
            path = f"abfss://{layer}@vmlakehouse.dfs.core.windows.net/erp/{name}"
            
        try:
            # Lấy timestamp mới nhất
            # Chú ý: Ở Gold, cột thời gian có thể khác _ingested_at (ví dụ _updated_at)
            time_col = "_updated_at" if layer == "gold" else "_ingested_at"
            
            latest_row = (
                spark.read.format("delta").load(path)
                .agg(spark_max(time_col).alias("latest"))
                .collect()
            )
            
            latest = latest_row[0]["latest"] if latest_row else None
            
            if latest:
                lag_minutes = (datetime.utcnow() - latest).total_seconds() / 60
                status = "OK" if lag_minutes <= sla_min else "SLA_BREACH"
                violations.append({
                    "table": table, 
                    "lag_minutes": round(lag_minutes, 1), 
                    "status": status,
                    "latest_time": latest.isoformat()
                })
            else:
                violations.append({"table": table, "lag_minutes": -1, "status": "NO_DATA"})
                
        except Exception as e:
            violations.append({"table": table, "lag_minutes": -1, "status": f"ERROR: {str(e)}"})

    # Báo cáo
    breaches = [v for v in violations if v["status"] != "OK"]
    
    print("\n" + "="*50)
    print("DATA FRESHNESS REPORT")
    print("="*50)
    for v in violations:
        print(f"[{v['status']}] {v['table']}: lag = {v['lag_minutes']} min (SLA: {SLA_MINUTES[v['table']]} min)")
        
    if breaches:
        print(f"\n[ALERT] {len(breaches)} SLA breach(es) detected!")
        return False
        
    print("\n[OK] All tables meet SLA.")
    return True

if __name__ == "__main__":
    is_ok = check_freshness()
    if not is_ok:
        sys.exit(1)
