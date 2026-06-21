"""
VinaMilk ERP Data Generator — Base Scenario
============================================
Base class for all business scenario generators.
Provides shared utilities:
  - SAP-style document number generation
  - Double-entry balance validation
  - Posting date / fiscal period calculation
  - User name simulation (VinaMilk SAP user IDs)
"""

import uuid
import random
from datetime import datetime, date, timedelta
from typing import Optional
from config import DOC_NUMBER_RANGES, FISCAL_YEAR, COMPANY_CODE, DEFAULT_FX_RATES

# ─────────────────────────────────────────────────────────
# SAP USER IDs — Realistic VinaMilk employee accounts
# ─────────────────────────────────────────────────────────
SAP_USERS = [
    "nguyen.van.a",    # Kế toán tổng hợp
    "tran.thi.b",      # Kế toán AR
    "le.van.c",        # Kế toán AP
    "pham.thi.d",      # Kế toán công nợ
    "hoang.van.e",     # Kế toán giá thành
    "vo.thi.f",        # Kế toán tiền lương
    "do.van.g",        # Kế toán tài sản
    "bui.thi.h",       # Kiểm soát nội bộ
    "dang.van.i",      # Trưởng phòng tài chính
    "BATCH_JOB",       # System automated posting (payroll, depreciation)
]

# VinaMilk SAP Purchase Organizations
PURCHASE_ORGS = ["P100", "P200", "P300"]  # HCM, HAN, Export

# Payment terms → due date calculation (days net)
PAYMENT_TERMS_DAYS = {
    "NET15": 15, "NET30": 30, "NET45": 45, "NET60": 60, "NET90": 90,
    "IMMEDIATE": 0, "END_MONTH": 30,
}


class BaseScenario:
    """Base class for all VinaMilk business scenario generators."""

    # Class-level document counters per type (resets each run)
    _doc_counters: dict = {}

    def __init__(self, master_data: dict):
        self.master_data  = master_data
        self.customers    = master_data.get("customers", [])
        self.vendors      = master_data.get("vendors", [])
        self.accounts     = master_data.get("accounts", {})
        self.cost_centers = master_data.get("cost_centers", [])
        self.plants       = master_data.get("plants", [])
        self.open_ar      = master_data.get("open_ar", [])
        self.open_ap      = master_data.get("open_ap", [])
        self.fx_rates     = DEFAULT_FX_RATES.copy()

    # ─────────────────────────────────────────────────────
    # DOCUMENT NUMBER — SAP sequential numbering per type
    # ─────────────────────────────────────────────────────
    def generate_doc_number(self, doc_type: str) -> str:
        """
        Generate SAP-style sequential document number.
        Format: 10-digit number within the doc type's number range.
        """
        if doc_type not in DOC_NUMBER_RANGES:
            doc_type = "SA"  # fallback

        range_start, range_end = DOC_NUMBER_RANGES[doc_type]

        if doc_type not in BaseScenario._doc_counters:
            BaseScenario._doc_counters[doc_type] = range_start + random.randint(0, 9999)

        BaseScenario._doc_counters[doc_type] += 1
        if BaseScenario._doc_counters[doc_type] > range_end:
            BaseScenario._doc_counters[doc_type] = range_start

        return str(BaseScenario._doc_counters[doc_type])

    # ─────────────────────────────────────────────────────
    # POSTING DATE — Realistic date near today
    # ─────────────────────────────────────────────────────
    def get_posting_date(self, max_days_back: int = 5) -> date:
        """
        Returns a posting date within the last N days.
        Simulates realistic posting lag (transactions posted 0-5 days after event).
        """
        days_back = random.randint(0, max_days_back)
        return date.today() - timedelta(days=days_back)

    def get_fiscal_period(self, posting_date: date) -> int:
        """Returns SAP fiscal period (month 1-12) from posting date."""
        return posting_date.month

    # ─────────────────────────────────────────────────────
    # HELPERS — Random selectors
    # ─────────────────────────────────────────────────────
    def pick_customer(self, channel: Optional[str] = None) -> dict:
        """Pick a random active customer, optionally filtered by channel type."""
        pool = self.customers
        if channel:
            pool = [c for c in pool if c.get("customer_type") == channel]
        if not pool:
            pool = self.customers
        return random.choice(pool)

    def pick_vendor(self, vendor_type: Optional[str] = None) -> dict:
        """Pick a random active vendor, optionally filtered by type."""
        pool = self.vendors
        if vendor_type:
            pool = [v for v in pool if v.get("vendor_type") == vendor_type]
        if not pool:
            pool = self.vendors
        return random.choice(pool)

    def pick_cost_center(self, cc_type: Optional[str] = None) -> Optional[dict]:
        """Pick a random cost center, optionally filtered by type."""
        pool = self.cost_centers
        if cc_type:
            pool = [cc for cc in pool if cc.get("cc_type") == cc_type]
        if not pool:
            pool = self.cost_centers
        return random.choice(pool) if pool else None

    def pick_plant(self) -> Optional[dict]:
        """Pick a random plant."""
        return random.choice(self.plants) if self.plants else None

    def pick_sap_user(self) -> str:
        """Pick a random SAP user ID."""
        return random.choice(SAP_USERS)

    def get_due_date(self, invoice_date: date, payment_terms: str) -> date:
        """Calculate due date based on payment terms."""
        days = PAYMENT_TERMS_DAYS.get(payment_terms, 30)
        return invoice_date + timedelta(days=days)

    def to_vnd(self, amount: float, currency: str) -> float:
        """Convert foreign currency amount to VND."""
        rate = self.fx_rates.get(currency, 1.0)
        return round(amount * rate, 2)

    def make_invoice_number(self, prefix: str, doc_number: str) -> str:
        """Generate a Vietnamese-style invoice number."""
        today = date.today()
        return f"{prefix}{today.strftime('%y%m')}-{doc_number[-6:]}"

    def make_po_number(self) -> str:
        """Generate a SAP Purchase Order number."""
        return f"45{random.randint(10000000, 19999999)}"

    # ─────────────────────────────────────────────────────
    # DOUBLE-ENTRY VALIDATION
    # ─────────────────────────────────────────────────────
    def validate_double_entry(self, gl_lines: list) -> bool:
        """
        Validates that total debits == total credits for a document.
        This is the fundamental accounting equation — must always balance.
        """
        total_debit  = sum(line["amount"] for line in gl_lines if line["debit_credit"] == "D")
        total_credit = sum(line["amount"] for line in gl_lines if line["debit_credit"] == "C")
        delta = abs(total_debit - total_credit)
        return delta < 0.01  # Allow 1 dong rounding tolerance

    def build_header(self, doc_type: str, currency: str, posting_date: date,
                     document_date: date, gl_lines: list, reference: str = "",
                     header_text: str = "", exchange_rate: float = 1.0) -> dict:
        """Build the transaction header dict."""
        txn_id = str(uuid.uuid4())
        doc_number = self.generate_doc_number(doc_type)
        fiscal_period = self.get_fiscal_period(posting_date)

        total_debit  = sum(l["amount"] for l in gl_lines if l["debit_credit"] == "D")
        total_credit = sum(l["amount"] for l in gl_lines if l["debit_credit"] == "C")

        # Assign txn_id to all GL lines
        for line in gl_lines:
            line["txn_id"] = txn_id

        return {
            "txn_id":        txn_id,
            "doc_number":    doc_number,
            "doc_type":      doc_type,
            "company_code":  COMPANY_CODE,
            "fiscal_year":   posting_date.year,
            "fiscal_period": fiscal_period,
            "posting_date":  posting_date,
            "document_date": document_date,
            "reference":     reference or "",
            "header_text":   header_text or "",
            "currency":      currency,
            "exchange_rate": exchange_rate,
            "total_debit":   total_debit,
            "total_credit":  total_credit,
            "status":        "POSTED",
            "created_by":    self.pick_sap_user(),
            "source_system": "SAP_MOCK",
        }

    def make_gl_line(self, line_item: int, account_id: str, debit_credit: str,
                     amount: float, txn_id: str = "", **kwargs) -> dict:
        """Build a GL line item dict."""
        return {
            "txn_id":       txn_id,
            "line_item":    line_item,
            "account_id":   account_id,
            "debit_credit": debit_credit,
            "amount":       round(amount, 2),
            "amount_vnd":   kwargs.get("amount_vnd"),
            "cost_center":  kwargs.get("cost_center"),
            "plant":        kwargs.get("plant"),
            "customer_id":  kwargs.get("customer_id"),
            "vendor_id":    kwargs.get("vendor_id"),
            "tax_code":     kwargs.get("tax_code"),
            "assignment":   kwargs.get("assignment"),
            "item_text":    kwargs.get("item_text", ""),
            "profit_center": kwargs.get("profit_center"),
        }

    def generate(self) -> dict:
        """Override in subclass to generate specific document type."""
        raise NotImplementedError("Subclasses must implement generate()")
