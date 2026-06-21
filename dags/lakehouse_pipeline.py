"""
VinaMilk Lakehouse — Airflow Orchestration DAG
───────────────────────────────────────────────
Lịch chạy: mỗi 15 phút (*/15 * * * *)

Thứ tự thực thi:
  1. [Silver Batch]  spark-submit silver_batch.py
                     (Bronze Delta → Silver Delta, MERGE + DQ rules)
  2. [Gold dbt run]  dbt run --profiles-dir /opt/dbt_gold
                     (Silver Delta → Gold Delta via DuckDB + dbt-duckdb)
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
    " --master local[*]"
    " --packages io.delta:delta-spark_2.12:3.2.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-azure:3.3.4"
    " --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
    " --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    " /opt/spark-jobs/silver_batch.py"
)

DBT_RUN = (
    "/home/airflow/.local/bin/dbt run"
    " --project-dir /opt/dbt_gold"
    " --profiles-dir /opt/dbt_gold"
    " --target dev"
)

# ─────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────
with DAG(
    dag_id="vinamik_lakehouse_pipeline",
    description="VinaMilk Medallion Lakehouse: Silver (PySpark) → Gold (dbt-duckdb)",
    schedule_interval="*/15 * * * *",       # Mỗi 15 phút
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,                       # Không chạy song song
    default_args=DEFAULT_ARGS,
    tags=["vinamik", "lakehouse", "medallion"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Task 1: Silver Batch (PySpark) ──────────────────
    silver_batch = BashOperator(
        task_id="silver_batch",
        bash_command=SPARK_SUBMIT,
        # Truyền env vars từ Airflow vào process
        env={
            "AZURE_STORAGE_ACCOUNT_NAME": os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", ""),
            "AZURE_STORAGE_ACCOUNT_KEY":  os.environ.get("AZURE_STORAGE_ACCOUNT_KEY", ""),
            "EVENT_HUBS_CONNECTION_STRING": os.environ.get("EVENT_HUBS_CONNECTION_STRING", ""),
        },
        doc_md="""
        ### Silver Batch
        Đọc Bronze Delta tables từ ADLS Gen2, áp dụng Data Quality rules,
        ghi ngược lại Silver Delta tables bằng MERGE (Upsert).
        """,
    )

    # ── Task 2: Gold dbt run (dbt-duckdb) ───────────────
    gold_dbt = BashOperator(
        task_id="gold_dbt_run",
        bash_command=DBT_RUN,
        env={
            "AZURE_STORAGE_ACCOUNT_NAME": os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", ""),
            "AZURE_STORAGE_ACCOUNT_KEY":  os.environ.get("AZURE_STORAGE_ACCOUNT_KEY", ""),
            # DuckDB azure extension dùng connection string
            "AZURE_STORAGE_CONNECTION_STRING": (
                "DefaultEndpointsProtocol=https;"
                f"AccountName={os.environ.get('AZURE_STORAGE_ACCOUNT_NAME', '')};"
                f"AccountKey={os.environ.get('AZURE_STORAGE_ACCOUNT_KEY', '')};"
                "EndpointSuffix=core.windows.net"
            ),
        },
        doc_md="""
        ### Gold dbt run
        Chạy toàn bộ dbt models trong dbt_gold/ qua engine DuckDB (in-process).
        DuckDB đọc Silver Delta trên ADLS Gen2, tính toán 6 bảng KPI Gold,
        ghi lại kết quả xuống Gold container trên ADLS Gen2.
        Models:
          - revenue_by_product_gold
          - ar_aging_gold
          - ap_aging_gold
          - budget_vs_actual_gold
          - gl_trial_balance_gold
          - cash_flow_summary_gold
        """,
    )

    end = EmptyOperator(task_id="end")

    # ── Thứ tự phụ thuộc ────────────────────────────────
    # start → silver_batch → gold_dbt_run → end
    start >> silver_batch >> gold_dbt >> end
