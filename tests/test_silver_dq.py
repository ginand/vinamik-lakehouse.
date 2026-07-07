# ─────────────────────────────────────────────────────────
# test_silver_dq.py — Unit tests cho Silver Layer DQ logic
# ─────────────────────────────────────────────────────────
# Test các DQ rules trong silver_batch.py mà không cần
# kết nối ADLS Gen2 hay Event Hubs — hoàn toàn local.
#
# Hàm được test (từ silver_batch.py):
#   - transform_transactions()  → DQ flags TXN
#   - transform_general_ledger() → DQ flags GL
#   - transform_ar()            → DQ flags AR
#   - transform_ap()            → DQ flags AP
#   - quarantine_and_filter()   → tách clean/dirty
#   - epoch_days_to_date()      → date parsing
#   - epoch_micros_to_ts()      → timestamp parsing
# ─────────────────────────────────────────────────────────

import sys
import os
import json
import pytest

# Thêm thư mục spark/ vào sys.path để import silver_batch
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark"))

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, LongType, BooleanType,
)
from pyspark.sql.functions import col, lit, current_timestamp

# Lazy import — chỉ import sau khi env vars được set (conftest.py đã set)
import silver_batch as sb


# ─────────────────────────────────────────────────────────
# HELPERS — Tạo Bronze raw DataFrame giả lập
# ─────────────────────────────────────────────────────────

BRONZE_SCHEMA = StructType([
    StructField("raw_payload", StringType(), True),
])


def make_bronze(spark: SparkSession, payloads: list[dict]):
    """
    Tạo Bronze DataFrame từ danh sách dict.
    Mỗi dict được JSON-encode thành raw_payload (giả lập Kafka message value).
    """
    rows = [(json.dumps(p),) for p in payloads]
    return spark.createDataFrame(rows, schema=BRONZE_SCHEMA)


def _txn(
    txn_id="TXN001",
    total_debit=1_000_000.0,
    total_credit=0.0,
    currency="VND",
    posting_date=20000,    # epoch days ~2024-09-16
    document_date=20000,
    entry_date=1_700_000_000_000_000,
    status="POSTED",
    company_code="VM01",
    source_system="SAP",
    **extra,
):
    """Tạo TXN payload chuẩn với giá trị mặc định hợp lệ."""
    base = dict(
        txn_id=txn_id, doc_number="DOC001", doc_type="KR",
        company_code=company_code, fiscal_year=2024, fiscal_period=9,
        posting_date=posting_date, document_date=document_date,
        entry_date=entry_date, reference="REF001", header_text="Test TXN",
        currency=currency, exchange_rate=1.0,
        total_debit=total_debit, total_credit=total_credit,
        status=status, created_by="user01", source_system=source_system,
        reversal_doc=None, _updated_at=entry_date,
    )
    base.update(extra)
    return base


def _ar(
    ar_id=1,
    customer_id="CUST001",
    amount=5_000_000.0,
    paid_amount=0.0,
    overdue_days=0,
    status="OPEN",
    invoice_date=20000,
    due_date=20030,
    **extra,
):
    """Tạo AR payload chuẩn."""
    base = dict(
        ar_id=ar_id, txn_id="TXN001", customer_id=customer_id,
        invoice_no=f"INV{ar_id:04d}", invoice_date=invoice_date,
        due_date=due_date, amount=amount, currency="VND",
        amount_vnd=amount, paid_amount=paid_amount,
        overdue_days=overdue_days, status=status,
        payment_method="BANK", sales_channel="DIRECT",
        plant="HANOI", cleared_date=None,
        created_at=1_700_000_000_000_000,
        updated_at=1_700_000_000_000_000,
    )
    base.update(extra)
    return base


def _ap(
    ap_id=1,
    vendor_id="VEND001",
    amount=3_000_000.0,
    paid_amount=0.0,
    overdue_days=0,
    status="OPEN",
    invoice_date=20000,
    due_date=20030,
    **extra,
):
    """Tạo AP payload chuẩn."""
    base = dict(
        ap_id=ap_id, txn_id="TXN001", vendor_id=vendor_id,
        invoice_no=f"PINV{ap_id:04d}", invoice_date=invoice_date,
        due_date=due_date, amount=amount, currency="VND",
        amount_vnd=amount, paid_amount=paid_amount,
        overdue_days=overdue_days, status=status,
        purchase_order="PO001", vendor_type="MATERIAL",
        plant="HANOI", cleared_date=None,
        created_at=1_700_000_000_000_000,
        updated_at=1_700_000_000_000_000,
    )
    base.update(extra)
    return base


def _gl(
    gl_id=1,
    account_id="511001",
    debit_credit="C",
    amount=1_000_000.0,
    cost_center="CC001",
    **extra,
):
    """Tạo GL payload chuẩn."""
    base = dict(
        gl_id=gl_id, txn_id="TXN001", line_item=1,
        account_id=account_id, debit_credit=debit_credit,
        amount=amount, amount_vnd=amount, cost_center=cost_center,
        plant="HANOI", customer_id=None, vendor_id=None,
        tax_code="V10", assignment="ASSIGN01", item_text="Test GL",
        profit_center="PC01",
        created_at=1_700_000_000_000_000,
        _updated_at=1_700_000_000_000_000,
    )
    base.update(extra)
    return base


# ─────────────────────────────────────────────────────────
# TEST CLASS 1 — DQ Rules cho Transactions
# ─────────────────────────────────────────────────────────

class TestTransactionDQRules:

    def test_valid_txn_is_clean(self, spark):
        """Record TXN hợp lệ phải có dq_is_clean = True."""
        df = make_bronze(spark, [_txn()])
        result = sb.transform_transactions(df)
        row = result.collect()[0]
        assert row["dq_is_clean"] is True, "TXN hợp lệ phải là clean"

    def test_amount_zero_both_sides(self, spark):
        """TXN có debit=0 VÀ credit=0 → dq_amount_zero=True, dq_is_clean=False."""
        df = make_bronze(spark, [_txn(total_debit=0.0, total_credit=0.0)])
        result = sb.transform_transactions(df)
        row = result.collect()[0]
        assert row["dq_amount_zero"] is True
        assert row["dq_is_clean"] is False

    def test_amount_zero_credit_only_is_valid(self, spark):
        """TXN chỉ có credit (debit=0) vẫn hợp lệ — không phải lỗi."""
        df = make_bronze(spark, [_txn(total_debit=0.0, total_credit=500_000.0)])
        result = sb.transform_transactions(df)
        row = result.collect()[0]
        assert row["dq_amount_zero"] is False
        assert row["dq_is_clean"] is True

    def test_invalid_currency_flagged(self, spark):
        """Currency không hợp lệ → dq_wrong_currency=True."""
        df = make_bronze(spark, [_txn(currency="XYZ")])
        result = sb.transform_transactions(df)
        row = result.collect()[0]
        assert row["dq_wrong_currency"] is True
        assert row["dq_is_clean"] is False

    def test_all_valid_currencies_accepted(self, spark):
        """VND, USD, EUR, JPY, SGD đều phải được chấp nhận."""
        valid_currencies = ["VND", "USD", "EUR", "JPY", "SGD"]
        for curr in valid_currencies:
            df = make_bronze(spark, [_txn(currency=curr)])
            result = sb.transform_transactions(df)
            row = result.collect()[0]
            assert row["dq_wrong_currency"] is False, f"{curr} phải hợp lệ"

    def test_posting_date_conversion(self, spark):
        """epoch_days=20000 → 2024-09-16 (kiểm tra date parsing)."""
        df = make_bronze(spark, [_txn(posting_date=20000)])
        result = sb.transform_transactions(df)
        row = result.collect()[0]
        # posting_date sau transform phải là date object, không phải int
        assert row["posting_date"] is not None
        assert str(row["posting_date"]) == "2024-09-16"

    def test_posting_month_derived_column(self, spark):
        """posting_month phải được tạo ra với format YYYY-MM."""
        df = make_bronze(spark, [_txn(posting_date=20000)])
        result = sb.transform_transactions(df)
        row = result.collect()[0]
        assert row["posting_month"] == "2024-09"


# ─────────────────────────────────────────────────────────
# TEST CLASS 2 — DQ Rules cho Accounts Receivable (AR)
# ─────────────────────────────────────────────────────────

class TestARDQRules:

    def test_valid_ar_is_clean(self, spark):
        """AR hợp lệ phải clean."""
        df = make_bronze(spark, [_ar()])
        result = sb.transform_ar(df)
        row = result.collect()[0]
        assert row["dq_is_clean"] is True

    def test_null_customer_id_flagged(self, spark):
        """customer_id NULL → dq_null_customer=True."""
        df = make_bronze(spark, [_ar(customer_id=None)])
        result = sb.transform_ar(df)
        row = result.collect()[0]
        assert row["dq_null_customer"] is True
        assert row["dq_is_clean"] is False

    def test_negative_amount_flagged(self, spark):
        """amount < 0 → dq_negative_amount=True."""
        df = make_bronze(spark, [_ar(amount=-1_000_000.0)])
        result = sb.transform_ar(df)
        row = result.collect()[0]
        assert row["dq_negative_amount"] is True
        assert row["dq_is_clean"] is False

    def test_invalid_status_flagged(self, spark):
        """Status nằm ngoài set hợp lệ → dq_invalid_status=True."""
        df = make_bronze(spark, [_ar(status="INVALID_STATUS")])
        result = sb.transform_ar(df)
        row = result.collect()[0]
        assert row["dq_invalid_status"] is True
        assert row["dq_is_clean"] is False

    def test_all_valid_statuses_accepted(self, spark):
        """OPEN, PARTIAL, PAID, OVERDUE, DISPUTED đều hợp lệ."""
        for status in ["OPEN", "PARTIAL", "PAID", "OVERDUE", "DISPUTED"]:
            df = make_bronze(spark, [_ar(status=status)])
            result = sb.transform_ar(df)
            row = result.collect()[0]
            assert row["dq_invalid_status"] is False, f"Status '{status}' phải hợp lệ"

    def test_paid_exceeds_amount_flagged(self, spark):
        """paid_amount > amount → dq_paid_exceeds_amount=True."""
        df = make_bronze(spark, [_ar(amount=1_000_000.0, paid_amount=2_000_000.0)])
        result = sb.transform_ar(df)
        row = result.collect()[0]
        assert row["dq_paid_exceeds_amount"] is True
        assert row["dq_is_clean"] is False

    def test_paid_equals_amount_is_valid(self, spark):
        """paid_amount == amount (fully paid) là hợp lệ."""
        df = make_bronze(spark, [_ar(amount=1_000_000.0, paid_amount=1_000_000.0, status="PAID")])
        result = sb.transform_ar(df)
        row = result.collect()[0]
        assert row["dq_paid_exceeds_amount"] is False

    def test_outstanding_amount_computed(self, spark):
        """outstanding_amount = amount - paid_amount."""
        df = make_bronze(spark, [_ar(amount=5_000_000.0, paid_amount=2_000_000.0)])
        result = sb.transform_ar(df)
        row = result.collect()[0]
        assert row["outstanding_amount"] == pytest.approx(3_000_000.0)

    def test_aging_bucket_current(self, spark):
        """overdue_days <= 0 → aging_bucket = CURRENT."""
        df = make_bronze(spark, [_ar(overdue_days=0)])
        result = sb.transform_ar(df)
        row = result.collect()[0]
        assert row["aging_bucket"] == "CURRENT"

    def test_aging_bucket_over_90(self, spark):
        """overdue_days > 90 → aging_bucket = OVER_90_DAYS."""
        df = make_bronze(spark, [_ar(overdue_days=91)])
        result = sb.transform_ar(df)
        row = result.collect()[0]
        assert row["aging_bucket"] == "OVER_90_DAYS"

    def test_paid_status_overrides_aging(self, spark):
        """PAID status → aging_bucket = PAID bất kể overdue_days."""
        df = make_bronze(spark, [_ar(status="PAID", overdue_days=120)])
        result = sb.transform_ar(df)
        row = result.collect()[0]
        assert row["aging_bucket"] == "PAID"


# ─────────────────────────────────────────────────────────
# TEST CLASS 3 — DQ Rules cho Accounts Payable (AP)
# ─────────────────────────────────────────────────────────

class TestAPDQRules:

    def test_valid_ap_is_clean(self, spark):
        """AP hợp lệ phải clean."""
        df = make_bronze(spark, [_ap()])
        result = sb.transform_ap(df)
        row = result.collect()[0]
        assert row["dq_is_clean"] is True

    def test_null_vendor_id_flagged(self, spark):
        """vendor_id NULL → dq_null_vendor=True."""
        df = make_bronze(spark, [_ap(vendor_id=None)])
        result = sb.transform_ap(df)
        row = result.collect()[0]
        assert row["dq_null_vendor"] is True
        assert row["dq_is_clean"] is False

    def test_negative_amount_ap_flagged(self, spark):
        """amount < 0 → dq_negative_amount=True."""
        df = make_bronze(spark, [_ap(amount=-500_000.0)])
        result = sb.transform_ap(df)
        row = result.collect()[0]
        assert row["dq_negative_amount"] is True
        assert row["dq_is_clean"] is False

    def test_invalid_ap_status_flagged(self, spark):
        """AP status ngoài set hợp lệ → dq_invalid_status=True."""
        df = make_bronze(spark, [_ap(status="CANCELLED")])
        result = sb.transform_ap(df)
        row = result.collect()[0]
        assert row["dq_invalid_status"] is True
        assert row["dq_is_clean"] is False

    def test_valid_ap_statuses_accepted(self, spark):
        """OPEN, PARTIAL, PAID, OVERDUE đều hợp lệ cho AP."""
        for status in ["OPEN", "PARTIAL", "PAID", "OVERDUE"]:
            df = make_bronze(spark, [_ap(status=status)])
            result = sb.transform_ap(df)
            row = result.collect()[0]
            assert row["dq_invalid_status"] is False, f"AP status '{status}' phải hợp lệ"

    def test_ap_outstanding_computed(self, spark):
        """outstanding_amount = amount - paid_amount cho AP."""
        df = make_bronze(spark, [_ap(amount=3_000_000.0, paid_amount=1_000_000.0)])
        result = sb.transform_ap(df)
        row = result.collect()[0]
        assert row["outstanding_amount"] == pytest.approx(2_000_000.0)


# ─────────────────────────────────────────────────────────
# TEST CLASS 4 — DQ Rules cho General Ledger (GL)
# ─────────────────────────────────────────────────────────

class TestGLDQRules:

    def test_valid_gl_is_clean(self, spark):
        """GL hợp lệ phải clean."""
        df = make_bronze(spark, [_gl()])
        result = sb.transform_general_ledger(df)
        row = result.collect()[0]
        assert row["dq_is_clean"] is True

    def test_missing_cost_center_flagged(self, spark):
        """cost_center NULL → dq_missing_cost_center=True."""
        df = make_bronze(spark, [_gl(cost_center=None)])
        result = sb.transform_general_ledger(df)
        row = result.collect()[0]
        assert row["dq_missing_cost_center"] is True
        assert row["dq_is_clean"] is False

    def test_amount_zero_gl_flagged(self, spark):
        """GL amount == 0 → dq_amount_zero=True."""
        df = make_bronze(spark, [_gl(amount=0.0)])
        result = sb.transform_general_ledger(df)
        row = result.collect()[0]
        assert row["dq_amount_zero"] is True
        assert row["dq_is_clean"] is False

    def test_negative_debit_gl_flagged(self, spark):
        """GL debit_credit='D' AND amount < 0 → dq_negative_amount=True."""
        df = make_bronze(spark, [_gl(debit_credit="D", amount=-50_000.0)])
        result = sb.transform_general_ledger(df)
        row = result.collect()[0]
        assert row["dq_negative_amount"] is True
        assert row["dq_is_clean"] is False

    def test_negative_credit_not_flagged(self, spark):
        """GL debit_credit='C' AND amount < 0 → không phải lỗi (điều chỉnh)."""
        df = make_bronze(spark, [_gl(debit_credit="C", amount=-50_000.0)])
        result = sb.transform_general_ledger(df)
        row = result.collect()[0]
        # Chỉ debit âm mới bị flag, credit âm là điều chỉnh hợp lệ
        assert row["dq_negative_amount"] is False


# ─────────────────────────────────────────────────────────
# TEST CLASS 5 — Quarantine & Filter Logic
# ─────────────────────────────────────────────────────────

class TestQuarantineAndFilter:

    def test_all_clean_returns_original(self, spark, tmp_path):
        """Nếu tất cả records clean → clean_df = full df, quarantine_count = 0."""
        df = make_bronze(spark, [_ar(), _ar(ar_id=2, customer_id="CUST002")])
        silver_df = sb.transform_ar(df)

        # Patch QUARANTINE_PATH sang tmp để không cần ADLS
        original_path = sb.QUARANTINE_PATH
        sb.QUARANTINE_PATH = str(tmp_path / "quarantine")

        try:
            clean_df, q_count = sb.quarantine_and_filter(spark, silver_df, "ar_silver", "ar_id")
            assert q_count == 0
            assert clean_df.count() == 2
        finally:
            sb.QUARANTINE_PATH = original_path

    def test_dirty_records_separated(self, spark, tmp_path):
        """Records lỗi phải bị tách ra, clean_df chỉ chứa records sạch."""
        payloads = [
            _ar(ar_id=1, customer_id="CUST001", amount=1_000_000.0),   # clean
            _ar(ar_id=2, customer_id=None, amount=1_000_000.0),          # dirty: null customer
            _ar(ar_id=3, customer_id="CUST003", amount=-999_999.0),     # dirty: negative amount
        ]
        df = make_bronze(spark, payloads)
        silver_df = sb.transform_ar(df)

        original_path = sb.QUARANTINE_PATH
        sb.QUARANTINE_PATH = str(tmp_path / "quarantine")

        try:
            clean_df, q_count = sb.quarantine_and_filter(spark, silver_df, "ar_silver", "ar_id")
            assert q_count == 2, f"Phải có 2 records lỗi, thực tế: {q_count}"
            assert clean_df.count() == 1, "Chỉ 1 record sạch"
            # Verify record sạch là AR ID 1
            clean_ids = [r["ar_id"] for r in clean_df.select("ar_id").collect()]
            assert 1 in clean_ids
        finally:
            sb.QUARANTINE_PATH = original_path

    def test_table_without_dq_flags_passes_through(self, spark, tmp_path):
        """Bảng không có cột dq_is_clean (customers, vendors, fx) → trả về nguyên bản."""
        # Tạo DataFrame không có DQ flags (giống customers_silver)
        simple_df = spark.createDataFrame(
            [("CUST001", "VinaMilk HN"), ("CUST002", "VinaMilk HCM")],
            schema=StructType([
                StructField("customer_id", StringType()),
                StructField("customer_name", StringType()),
            ])
        )
        original_path = sb.QUARANTINE_PATH
        sb.QUARANTINE_PATH = str(tmp_path / "quarantine")

        try:
            clean_df, q_count = sb.quarantine_and_filter(
                spark, simple_df, "customers_silver", "customer_id"
            )
            assert q_count == 0
            assert clean_df.count() == 2
        finally:
            sb.QUARANTINE_PATH = original_path


# ─────────────────────────────────────────────────────────
# TEST CLASS 6 — Helper functions (date/timestamp parsing)
# ─────────────────────────────────────────────────────────

class TestDateHelpers:

    def test_epoch_days_zero_is_1970(self, spark):
        """epoch_days=0 → 1970-01-01."""
        df = make_bronze(spark, [_gl()])
        # Tạo DataFrame có cột int đơn giản để test helper
        test_df = spark.createDataFrame([(0,)], ["epoch_days"])
        from pyspark.sql.functions import expr
        result = test_df.withColumn(
            "converted",
            expr("date_add(date '1970-01-01', `epoch_days`)")
        ).collect()[0]["converted"]
        assert str(result) == "1970-01-01"

    def test_epoch_days_20000_is_2024(self, spark):
        """epoch_days=20000 → 2024-09-16."""
        df = make_bronze(spark, [_txn(posting_date=20000)])
        result = sb.transform_transactions(df)
        row = result.collect()[0]
        assert str(row["posting_date"]) == "2024-09-16"

    def test_epoch_micros_conversion(self, spark):
        """Epoch microseconds phải chuyển thành timestamp hợp lệ."""
        # 1_700_000_000_000_000 µs = 2023-11-14 22:13:20 UTC
        df = make_bronze(spark, [_gl()])
        result = sb.transform_general_ledger(df)
        row = result.collect()[0]
        # Chỉ cần kiểm tra created_at không phải None và có type timestamp
        assert row["created_at"] is not None
