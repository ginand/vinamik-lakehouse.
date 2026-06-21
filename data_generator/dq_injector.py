"""
VinaMilk ERP — Data Quality Error Injector
==========================================
Injects realistic SAP-style data quality errors into accounting documents.

Error types mirror real production SAP issues:
  1. missing_cost_center:  Kế toán quên điền Cost Center cho P&L accounts
  2. duplicate_posting:    User nhấn Submit 2 lần → trùng doc number
  3. amount_zero:          Posting zero amount (clearing error)
  4. invalid_gl_account:   Gõ sai số tài khoản (e.g., 512 thay vì 511)
  5. wrong_currency:       Copy template cũ với currency sai (SGD/THB)
  6. future_posting_date:  System clock lệch / pre-dated entries
  7. negative_amount:      Credit memo post vào wrong account

Target: 15-20% total error rate for Great Expectations + Power BI DQ Monitor.

Each errored document gets: _dq_error_injected=True, _dq_error_type=<type>
"""

import random
import copy
from datetime import date, timedelta
from config import DQ_ERROR_RATES

# Invalid GL account numbers (look like real accounts but don't exist in VAS)
INVALID_GL_ACCOUNTS = ["512", "5110", "133", "335", "631", "6311", "1320", "3319"]

# Invalid currency codes (common copy-paste mistakes in VinaMilk: SGD, THB from export templates)
INVALID_CURRENCIES = ["SGD", "THB", "KRW", "TWD", "MYR", "INR"]


class DQInjector:
    """
    Data Quality error injector — wraps accounting documents
    and probabilistically injects realistic errors.
    """

    def __init__(self):
        self.error_rates = DQ_ERROR_RATES
        self.total_injected = 0
        self.error_counts = {k: 0 for k in self.error_rates}

    def maybe_inject_error(self, doc: dict) -> dict:
        """
        Probabilistically injects ONE error type per document.
        Returns modified doc with _dq_error_injected flag.
        """
        for error_type, rate in self.error_rates.items():
            if random.random() < rate:
                doc = self._inject(doc, error_type)
                doc["_dq_error_injected"] = True
                doc["_dq_error_type"]     = error_type
                self.total_injected += 1
                self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1
                return doc  # One error per document (realistic)

        doc["_dq_error_injected"] = False
        doc["_dq_error_type"]     = None
        return doc

    # ─────────────────────────────────────────────────────
    # ERROR INJECTION METHODS
    # ─────────────────────────────────────────────────────

    def _inject(self, doc: dict, error_type: str) -> dict:
        """Route to specific error injection method."""
        methods = {
            "missing_cost_center":  self._inject_missing_cost_center,
            "duplicate_posting":    self._inject_duplicate_posting,
            "amount_zero":          self._inject_amount_zero,
            "invalid_gl_account":   self._inject_invalid_gl_account,
            "wrong_currency":       self._inject_wrong_currency,
            "future_posting_date":  self._inject_future_date,
            "negative_amount":      self._inject_negative_amount,
        }
        method = methods.get(error_type)
        if method:
            try:
                return method(copy.deepcopy(doc))
            except Exception:
                # If injection fails, return doc with error flag but unchanged
                pass
        return doc

    def _inject_missing_cost_center(self, doc: dict) -> dict:
        """
        Error: Kế toán quên điền cost center cho P&L accounts.
        Affects revenue/expense lines (accounts 5xx, 6xx).
        Real cause: Manual posting without required field check.
        """
        for line in doc.get("gl_lines", []):
            account_id = line.get("account_id", "")
            # Remove cost center from P&L accounts that REQUIRE it
            if account_id and account_id[0] in ("5", "6"):
                line["cost_center"] = None  # Missing CC → Great Expectations will catch this
        return doc

    def _inject_duplicate_posting(self, doc: dict) -> dict:
        """
        Error: User submits the same document twice.
        Simulated by using a previously-used document number.
        Real cause: Browser timeout, double-click, network error → resubmit.
        """
        header = doc.get("header", {})
        doc_type = header.get("doc_type", "SA")

        # Use a number from the START of the range (simulating an already-used number)
        from config import DOC_NUMBER_RANGES
        range_start, _ = DOC_NUMBER_RANGES.get(doc_type, (1900000001, 1999999999))
        header["doc_number"] = str(range_start + random.randint(1, 100))
        return doc

    def _inject_amount_zero(self, doc: dict) -> dict:
        """
        Error: GL line with zero amount.
        Real cause: Clearing entry with wrong sign, or partial-clear that results in 0.
        This breaks the "amount must be > 0" rule.
        """
        gl_lines = doc.get("gl_lines", [])
        if gl_lines:
            target_line = random.choice(gl_lines)
            target_line["amount"]     = 0.00
            target_line["amount_vnd"] = 0.00
        return doc

    def _inject_invalid_gl_account(self, doc: dict) -> dict:
        """
        Error: GL account doesn't exist in chart of accounts.
        Real cause: Kế toán gõ sai số tài khoản khi post manual entry.
        Example: typed "512" instead of "511", or "133" instead of "1331".
        """
        gl_lines = doc.get("gl_lines", [])
        if gl_lines:
            target_line = random.choice(gl_lines)
            invalid_acc = random.choice(INVALID_GL_ACCOUNTS)
            target_line["account_id"] = invalid_acc
        return doc

    def _inject_wrong_currency(self, doc: dict) -> dict:
        """
        Error: Wrong currency code in transaction.
        Real cause: Copying export invoice template → forgot to change SGD→VND.
        Great Expectations rule: currency must be in ['VND', 'USD', 'EUR', 'JPY', 'SGD'].
        """
        header = doc.get("header", {})
        invalid_currency = random.choice(INVALID_CURRENCIES)
        header["currency"] = invalid_currency

        # Also inject into GL lines to be consistent
        for line in doc.get("gl_lines", []):
            if random.random() < 0.5:
                line["amount_vnd"] = None  # FX amount missing for non-standard currency

        return doc

    def _inject_future_date(self, doc: dict) -> dict:
        """
        Error: Posting date is in the future.
        Real cause: System clock drift, or user deliberately pre-dates entries.
        Great Expectations rule: posting_date <= today().
        """
        header = doc.get("header", {})
        future_days = random.randint(1, 30)
        header["posting_date"] = date.today() + timedelta(days=future_days)
        # Fiscal period also becomes future
        header["fiscal_period"] = header["posting_date"].month
        return doc

    def _inject_negative_amount(self, doc: dict) -> dict:
        """
        Error: Negative amount on an AR/AP line.
        Real cause: Credit memo (credit note) posted to wrong document type.
        Should be a negative debit = credit, but posted as negative AR.
        Great Expectations rule: AR amount >= 0.
        """
        ar = doc.get("ar_record")
        if ar:
            ar["amount"] = -abs(ar["amount"])

        # Also negate a GL line
        gl_lines = doc.get("gl_lines", [])
        for line in gl_lines:
            if line.get("account_id") == "131":  # AR line
                line["amount"] = -abs(line["amount"])
                break

        return doc

    # ─────────────────────────────────────────────────────
    # STATISTICS
    # ─────────────────────────────────────────────────────
    def get_stats(self) -> dict:
        return {
            "total_errors_injected": self.total_injected,
            "by_type": self.error_counts,
        }
