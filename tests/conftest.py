# ─────────────────────────────────────────────────────────
# conftest.py — Shared Spark Session fixture cho toàn bộ test suite
# ─────────────────────────────────────────────────────────
# Cách chạy:
#   pip install pyspark==3.5.0 delta-spark==3.2.1 pytest pytest-cov
#   pytest tests/ -v --cov=spark/ --cov-report=term-missing
# ─────────────────────────────────────────────────────────

import os
import pytest

os.environ.setdefault("AZURE_STORAGE_ACCOUNT_NAME", "test_account")
os.environ.setdefault("AZURE_STORAGE_ACCOUNT_KEY", "dGVzdF9rZXk=")  # base64 dummy

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    SparkSession dùng chung cho mọi test.
    scope="session" → khởi tạo 1 lần duy nhất, tái dùng cho toàn bộ suite.
    Dùng Delta Lake extension nhưng KHÔNG kết nối Azure (local only).
    """
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("vinamik-unit-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "512m")
        .config("spark.ui.enabled", "false")   # Tắt Spark UI để test nhanh hơn
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
