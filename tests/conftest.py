# ─────────────────────────────────────────────────────────
# conftest.py — Shared Spark Session fixture cho toàn bộ test suite
# ─────────────────────────────────────────────────────────
# Cách chạy:
#   pip install pyspark==3.5.3 delta-spark==3.2.1 pytest pytest-cov
#   pytest tests/ -v --cov=spark/ --cov-report=term-missing
# ─────────────────────────────────────────────────────────

import os
import pytest

os.environ.setdefault("AZURE_STORAGE_ACCOUNT_NAME", "test_account")
os.environ.setdefault("AZURE_STORAGE_ACCOUNT_KEY", "dGVzdF9rZXk=")  # base64 dummy

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope="session")
def spark():
    """
    SparkSession dùng chung cho mọi test.
    scope="session" → khởi tạo 1 lần duy nhất, tái dùng cho toàn bộ suite.
    configure_spark_with_delta_pip() tự động load Delta Lake JARs vào JVM classpath.
    """
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("vinamik-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "512m")
        .config("spark.ui.enabled", "false")
        # Bắt buộc set để Delta pip utils tìm đúng JAR
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.1")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
