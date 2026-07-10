"""
VinaMilk Lakehouse — Airflow Orchestration DAG
───────────────────────────────────────────────
Lịch chạy: mỗi 15 phút (*/15 * * * *)

Thứ tự thực thi:
  1. [Silver Batch]    spark-submit silver_batch.py
                       (Bronze → Silver, MERGE + DQ rules + Quarantine)
  2. [DQ Health Check] spark-submit dq_health_check.py
                       (Kiểm tra quarantine, cảnh báo nếu error rate > 20%)
  3. [Gold dbt run]    dbt run --profiles-dir /opt/dbt_gold
                       (Silver → Gold via DuckDB + dbt-duckdb)
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# ─────────────────────────────────────────────────────────
# Default args
# ─────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "vinamik-data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ─────────────────────────────────────────────────────────
# Cấu hình đường dẫn (mount vào container Airflow)
# ─────────────────────────────────────────────────────────
SPARK_SUBMIT = (
    "/home/airflow/.local/bin/spark-submit"
    " --master local[2]"
    " --driver-memory 1g"
    " --packages io.delta:delta-spark_2.12:3.2.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-azure:3.3.4"
    " --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
    " --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    " /opt/spark-jobs/{script}"
)

DBT_RUN = (
    "export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt && "
    "export SSL_CERT_DIR=/etc/ssl/certs && "
    "export CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt && "
    "export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt && "
    "/home/airflow/.local/bin/dbt run"
    " --project-dir /opt/dbt_gold"
    " --profiles-dir /opt/dbt_gold"
    " --target dev"
)

# ─────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────
with DAG(
    dag_id="vinamik_erp_lakehouse",
    description="VinaMilk Medallion Lakehouse: Silver (PySpark) → Gold (dbt-duckdb)",
    schedule_interval="*/15 * * * *",       # Mỗi 15 phút
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,                       # Không chạy song song
    default_args=DEFAULT_ARGS,
    tags=["vinamik", "lakehouse", "medallion"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Lấy env vars và loại bỏ dấu nháy kép thừa (nếu có) do docker-compose truyền từ .env
    azure_acc_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", "").strip('"').strip("'")
    azure_acc_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY", "").strip('"').strip("'")
    event_hubs_conn = os.environ.get("EVENT_HUBS_CONNECTION_STRING", "").strip('"').strip("'")

    # ── Task 1: Silver Batch (PySpark) ──────────────────
    silver_batch = BashOperator(
        task_id="silver_batch",
        bash_command=SPARK_SUBMIT.format(script="silver_batch.py"),
        # Truyền env vars từ Airflow vào process
        env={
            **os.environ,
            "AZURE_STORAGE_ACCOUNT_NAME": azure_acc_name,
            "AZURE_STORAGE_ACCOUNT_KEY":  azure_acc_key,
            "EVENT_HUBS_CONNECTION_STRING": event_hubs_conn,
        },
        doc_md="""
        ### Silver Batch
        Đọc Bronze Delta tables từ ADLS Gen2, áp dụng Data Quality rules,
        cách ly records lỗi vào quarantine container,
        chỉ MERGE records sạch vào Silver Delta tables.
        """,
    )

    # ── Task 2: DQ Health Check (PySpark) ───────────────
    dq_health_check = BashOperator(
        task_id="dq_health_check",
        bash_command=SPARK_SUBMIT.format(script="dq_health_check.py"),
        env={
            **os.environ,
            "AZURE_STORAGE_ACCOUNT_NAME": azure_acc_name,
            "AZURE_STORAGE_ACCOUNT_KEY":  azure_acc_key,
        },
        doc_md="""
        ### Data Quality Health Check
        Đọc quarantine tables trên ADLS Gen2.
        Tổng hợp số records lỗi theo bảng và loại lỗi.
        **FAIL task nếu error rate > 20%** để cảnh báo team.
        """,
    )

    # ── Task 3: Great Expectations Validation (PySpark) ──
    gx_validation = BashOperator(
        task_id="gx_validation",
        bash_command=SPARK_SUBMIT.format(script="gx_health_check.py"),
        env={
            **os.environ,
            "AZURE_STORAGE_ACCOUNT_NAME": azure_acc_name,
            "AZURE_STORAGE_ACCOUNT_KEY":  azure_acc_key,
        },
        doc_md="""
        ### Great Expectations (GX)
        Chạy script gx_health_check.py để kiểm định dữ liệu Silver 
        và tự động sinh ra các file HTML (Data Docs) báo cáo chất lượng dữ liệu.
        """,
    )

    azure_conn_str = (
        "DefaultEndpointsProtocol=http;"
        f"AccountName={azure_acc_name};"
        f"AccountKey={azure_acc_key};"
        "EndpointSuffix=core.windows.net"
    )

    gold_dbt = BashOperator(
        task_id="gold_dbt_run",
        bash_command=DBT_RUN,
        env={
            **os.environ,
            "AZURE_STORAGE_ACCOUNT_NAME": azure_acc_name,
            "AZURE_STORAGE_ACCOUNT_KEY":  azure_acc_key,
            "AZURE_STORAGE_CONNECTION_STRING": azure_conn_str,
            "SSL_CERT_FILE":    "/etc/ssl/certs/ca-certificates.crt",
            "SSL_CERT_DIR":     "/etc/ssl/certs",
            "CURL_CA_BUNDLE":   "/etc/ssl/certs/ca-certificates.crt",
            "REQUESTS_CA_BUNDLE": "/etc/ssl/certs/ca-certificates.crt",
        },
        doc_md="""
        ### Gold dbt run
        Chạy toàn bộ dbt models trong dbt_gold/ qua engine DuckDB (in-process).
        Đã sử dụng DefaultEndpointsProtocol=http để vượt qua lỗi SSL.
        Models:
          - revenue_by_product_gold
          - ar_aging_gold
          - ap_aging_gold
          - budget_vs_actual_gold
          - gl_trial_balance_gold
          - cash_flow_summary_gold
          - dq_monitoring_gold   ← Data Quality dashboard
        """,
    )

    end = EmptyOperator(task_id="end")

    # ── Thứ tự phụ thuộc ────────────────────────────────
    # start → silver_batch → dq_health_check → gx_validation → gold_dbt_run → end
    start >> silver_batch >> dq_health_check >> gx_validation >> gold_dbt >> end
