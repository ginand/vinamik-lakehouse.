"""
VinaMilk ERP — Collections & Payments Scenarios
================================================
Scenario A - AR Collection (Thu tiền khách hàng):
  - Document type: DZ (Customer Payment)
  - Debit:  112x (Bank account - ngân hàng nhận tiền)
  - Credit: 131  (Clear AR - xóa công nợ phải thu)
  - Updates accounts_receivable status → PAID / PARTIAL

Scenario B - AP Payment (Thanh toán nhà cung cấp):
  - Document type: KZ (Vendor Payment)
  - Debit:  331  (Clear AP - xóa công nợ phải trả)
  - Credit: 112x (Bank account - chuyển khoản đi)
  - Updates accounts_payable status → PAID / PARTIAL

VinaMilk bank accounts (VAS Account 112):
  - 1121: VCB - Vietcombank (primary)
  - 1122: Techcombank
  - 1123: BIDV
  - 1124: VCB USD account
"""

import random
from datetime import date, timedelta
from scenarios.base_scenario import BaseScenario

# VinaMilk's bank accounts mapped to banks
BANK_ACCOUNTS = {
    "1121": "VCB - Ngân hàng Vietcombank",
    "1122": "Techcombank - VND",
    "1123": "BIDV - VND",
    "1124": "VCB - USD Account",
    "1125": "VCB - EUR Account",
}

BANK_WEIGHTS = [0.45, 0.25, 0.20, 0.07, 0.03]  # VCB is dominant


class CollectionScenario(BaseScenario):
    """
    Generates customer payment documents (DZ).
    Clears existing open AR invoices.
    """

    def generate(self) -> dict:
        open_ar = self.open_ar

        if not open_ar:
            # No open AR — generate a standalone bank receipt (edge case)
            return self._generate_standalone_receipt()

        # Pick an open AR to clear (prefer oldest → simulate real payment behavior)
        # Sort by due date: overdue items are paid first (or NEVER paid — for GE testing)
        ar_item = random.choice(open_ar[:50])  # Pick from top 50 oldest

        posting_date  = date.today() - timedelta(days=random.randint(0, 2))
        document_date = posting_date

        remaining = float(ar_item["amount"]) - float(ar_item.get("paid_amount", 0))
        if remaining <= 0:
            return self._generate_standalone_receipt()

        # Partial or full payment (70% full, 30% partial)
        if random.random() < 0.70:
            payment_amount = round(remaining, -3)
            new_status = "PAID"
        else:
            payment_amount = round(remaining * random.uniform(0.3, 0.9), -3)
            new_status = "PARTIAL"

        currency = ar_item.get("currency", "VND")
        fx_rate  = self.fx_rates.get(currency, 1.0)

        # Pick bank account (prefer matching currency)
        if currency == "USD":
            bank_account = "1124"
        elif currency == "EUR":
            bank_account = "1125"
        else:
            bank_account = random.choices(
                list(BANK_ACCOUNTS.keys())[:3],
                weights=BANK_WEIGHTS[:3],
                k=1
            )[0]

        bank_name = BANK_ACCOUNTS.get(bank_account, "VCB")

        gl_lines = [
            # Line 001: DEBIT 112x — Bank received payment
            self.make_gl_line(
                line_item=1, account_id=bank_account, debit_credit="D",
                amount=payment_amount,
                item_text=f"Thu tiền - {bank_name}",
                assignment=ar_item.get("invoice_no", ""),
            ),
            # Line 002: CREDIT 131 — Clear customer AR
            self.make_gl_line(
                line_item=2, account_id="131", debit_credit="C",
                amount=payment_amount,
                customer_id=ar_item["customer_id"],
                item_text=f"Xóa AR #{ar_item.get('invoice_no', '')}",
                assignment=ar_item.get("invoice_no", ""),
            ),
        ]

        assert self.validate_double_entry(gl_lines)

        header = self.build_header(
            doc_type      = "DZ",
            currency      = currency,
            posting_date  = posting_date,
            document_date = document_date,
            gl_lines      = gl_lines,
            reference     = ar_item.get("invoice_no", ""),
            header_text   = f"Thu tiền từ KH {ar_item['customer_id']}",
            exchange_rate = fx_rate,
        )

        new_paid = float(ar_item.get("paid_amount", 0)) + payment_amount

        ar_update = {
            "ar_id":        ar_item["ar_id"],
            "paid_amount":  new_paid,
            "status":       new_status,
            "cleared_date": posting_date if new_status == "PAID" else None,
        }

        return {
            "header":    header,
            "gl_lines":  gl_lines,
            "ar_update": ar_update,
        }

    def _generate_standalone_receipt(self) -> dict:
        """Fallback when no open AR: generate a bank interest receipt."""
        posting_date = date.today()
        amount = round(random.uniform(5_000_000, 50_000_000), -3)
        bank_account = "1121"

        gl_lines = [
            self.make_gl_line(
                line_item=1, account_id=bank_account, debit_credit="D",
                amount=amount, item_text="Lãi tiền gửi ngân hàng"
            ),
            self.make_gl_line(
                line_item=2, account_id="515", debit_credit="C",
                amount=amount, item_text="Doanh thu tài chính - lãi gửi"
            ),
        ]

        header = self.build_header(
            doc_type="SA", currency="VND", posting_date=posting_date,
            document_date=posting_date, gl_lines=gl_lines,
            header_text="Lãi tiền gửi ngân hàng",
        )
        return {"header": header, "gl_lines": gl_lines}


class PaymentScenario(BaseScenario):
    """
    Generates vendor payment documents (KZ).
    Clears existing open AP invoices.
    """

    def generate(self) -> dict:
        open_ap = self.open_ap

        if not open_ap:
            return self._generate_bank_fee()

        ap_item = random.choice(open_ap[:50])

        posting_date  = date.today() - timedelta(days=random.randint(0, 1))
        document_date = posting_date

        remaining = float(ap_item["amount"]) - float(ap_item.get("paid_amount", 0))
        if remaining <= 0:
            return self._generate_bank_fee()

        # Full payment (85%) or partial (15%)
        if random.random() < 0.85:
            payment_amount = round(remaining, -3 if ap_item.get("currency", "VND") == "VND" else 2)
            new_status = "PAID"
        else:
            payment_amount = round(remaining * random.uniform(0.4, 0.8), -3)
            new_status = "PARTIAL"

        currency = ap_item.get("currency", "VND")
        fx_rate  = self.fx_rates.get(currency, 1.0)

        bank_account = "1124" if currency == "USD" else "1121"
        bank_name    = BANK_ACCOUNTS.get(bank_account, "VCB")

        gl_lines = [
            # Line 001: DEBIT 331 — Clear vendor AP
            self.make_gl_line(
                line_item=1, account_id="331", debit_credit="D",
                amount=payment_amount,
                vendor_id=ap_item["vendor_id"],
                item_text=f"Thanh toán AP #{ap_item.get('invoice_no', '')}",
                assignment=ap_item.get("invoice_no", ""),
            ),
            # Line 002: CREDIT 112x — Bank payment sent
            self.make_gl_line(
                line_item=2, account_id=bank_account, debit_credit="C",
                amount=payment_amount,
                item_text=f"Chuyển khoản - {bank_name}",
                assignment=ap_item.get("invoice_no", ""),
            ),
        ]

        assert self.validate_double_entry(gl_lines)

        header = self.build_header(
            doc_type      = "KZ",
            currency      = currency,
            posting_date  = posting_date,
            document_date = document_date,
            gl_lines      = gl_lines,
            reference     = ap_item.get("invoice_no", ""),
            header_text   = f"Thanh toán NCC {ap_item['vendor_id']}",
            exchange_rate = fx_rate,
        )

        new_paid = float(ap_item.get("paid_amount", 0)) + payment_amount

        ap_update = {
            "ap_id":        ap_item["ap_id"],
            "paid_amount":  new_paid,
            "status":       new_status,
            "cleared_date": posting_date if new_status == "PAID" else None,
        }

        return {
            "header":    header,
            "gl_lines":  gl_lines,
            "ap_update": ap_update,
        }

    def _generate_bank_fee(self) -> dict:
        """Fallback: bank charge / interest expense."""
        posting_date = date.today()
        amount = round(random.uniform(1_000_000, 15_000_000), -3)

        gl_lines = [
            self.make_gl_line(
                line_item=1, account_id="635", debit_credit="D",
                amount=amount, item_text="Phí ngân hàng / lãi vay"
            ),
            self.make_gl_line(
                line_item=2, account_id="1121", debit_credit="C",
                amount=amount, item_text="VCB - trừ tự động"
            ),
        ]

        header = self.build_header(
            doc_type="SA", currency="VND", posting_date=posting_date,
            document_date=posting_date, gl_lines=gl_lines,
            header_text="Phí ngân hàng",
        )
        return {"header": header, "gl_lines": gl_lines}
