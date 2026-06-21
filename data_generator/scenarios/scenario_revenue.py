"""
VinaMilk ERP — Revenue Scenario (Xuất hóa đơn bán hàng)
=========================================================
Simulates VinaMilk's most common transaction type: billing for milk sales.

Two modes:
  1. DOMESTIC (export=False): Sales to MT/TT channels in VND
     - Document type: RV (Revenue Invoice from Billing)
     - Debit:  131 (AR - Phải thu KH)        full invoice amount incl. VAT
     - Credit: 511x (Revenue by product)      net amount
     - Credit: 3331 (VAT Output 10%)          10% of net

  2. EXPORT (export=True): Export sales in USD
     - Document type: RV (Revenue Invoice)
     - Debit:  131 (AR - Export Customer)     full USD amount
     - Credit: 5121 (Export Revenue)          net USD (VAT 0% exempt)
     - Credit: 3331 (VAT 0%)                  0 (export is VAT-exempt in VN)

Business Rules (VAS compliant):
  - VAT 10% for domestic sales (GTGT đầu ra)
  - VAT 0% for exports (Thông tư 219/2013/TT-BTC)
  - Cost center must match sales channel
  - Exchange rate recorded for FX transactions
"""

import random
from datetime import date, timedelta
from scenarios.base_scenario import BaseScenario
from config import PRODUCT_LINES, DEFAULT_FX_RATES


class RevenueScenario(BaseScenario):
    """Generates sales billing documents (RV) for VinaMilk product lines."""

    VAT_RATE_DOMESTIC = 0.10   # 10% VAT for domestic sales
    VAT_RATE_EXPORT   = 0.00   # 0% VAT for exports (exempt)

    # Typical invoice size by channel (in millions VND)
    INVOICE_SIZE_BY_CHANNEL = {
        "MT":     (500_000_000,  8_000_000_000),   # BigC order: 500M - 8B VND
        "TT":     (50_000_000,   800_000_000),     # Distributor order: 50M - 800M VND
        "EXPORT": (400_000_000,  3_000_000_000),   # Export: 400M - 3B VND
        "GT":     (20_000_000,   200_000_000),     # GT/HORECA: 20M - 200M VND
    }

    def __init__(self, master_data: dict, export: bool = False):
        super().__init__(master_data)
        self.export = export

    def generate(self) -> dict:
        """Generate one complete sales invoice document."""

        posting_date  = self.get_posting_date(max_days_back=3)
        document_date = posting_date - timedelta(days=random.randint(0, 2))

        if self.export:
            return self._generate_export_invoice(posting_date, document_date)
        else:
            return self._generate_domestic_invoice(posting_date, document_date)

    # ─────────────────────────────────────────────────────
    # DOMESTIC SALES (RV document in VND)
    # ─────────────────────────────────────────────────────
    def _generate_domestic_invoice(self, posting_date: date, document_date: date) -> dict:
        # Pick channel (weighted: MT is ~55% of domestic)
        channel = random.choices(
            ["MT", "TT", "GT"],
            weights=[0.55, 0.38, 0.07],
            k=1
        )[0]

        customer = self.pick_customer(channel)

        # Pick product line
        product_key = random.choices(
            list(PRODUCT_LINES.keys()),
            weights=[p["weight"] for p in PRODUCT_LINES.values()],
            k=1
        )[0]
        product = PRODUCT_LINES[product_key]

        # Invoice net amount (before VAT)
        min_amt, max_amt = self.INVOICE_SIZE_BY_CHANNEL.get(channel, (50_000_000, 500_000_000))
        net_amount  = round(random.uniform(min_amt, max_amt), -3)  # Round to 1000 VND
        vat_amount  = round(net_amount * self.VAT_RATE_DOMESTIC, -3)
        gross_total = net_amount + vat_amount

        # Cost center for the sale (match to channel)
        cc_type = "SALES"
        cc_pool = [cc for cc in self.cost_centers if cc.get("cc_type") == cc_type]
        cost_center = random.choice(cc_pool)["cost_center_id"] if cc_pool else None

        # Prefer plant in same region as customer
        plant_info = self.pick_plant()
        plant_id = plant_info["plant_id"] if plant_info else None

        # Build GL lines (3-line double entry)
        gl_lines = [
            # Line 001: DEBIT  131 — Phải thu khách hàng (full invoice incl. VAT)
            self.make_gl_line(
                line_item=1, account_id="131", debit_credit="D",
                amount=gross_total, currency="VND",
                customer_id=customer["customer_id"],
                item_text=f"AR - {customer['customer_name'][:50]}",
                assignment=self.make_invoice_number("VM", self.generate_doc_number("RV")),
                tax_code="V1",
            ),
            # Line 002: CREDIT 511x — Doanh thu (net amount)
            self.make_gl_line(
                line_item=2, account_id=product["account"], debit_credit="C",
                amount=net_amount,
                cost_center=cost_center,
                plant=plant_id,
                item_text=f"Bán {product['name']} - {channel}",
                tax_code="V1",
                profit_center=f"PC-{channel}",
            ),
            # Line 003: CREDIT 3331 — VAT Output 10%
            self.make_gl_line(
                line_item=3, account_id="3331", debit_credit="C",
                amount=vat_amount,
                item_text=f"GTGT đầu ra 10% - {customer['customer_name'][:30]}",
                tax_code="V1",
            ),
        ]

        assert self.validate_double_entry(gl_lines), "CRITICAL: Double-entry imbalance in revenue doc!"

        # Build invoice number (Vietnamese format: VM-YYMM-XXXXXX)
        invoice_no = self.make_invoice_number("VM", gl_lines[0].get("assignment", ""))
        due_date   = self.get_due_date(posting_date, customer.get("payment_terms", "NET30"))

        # Build header
        header = self.build_header(
            doc_type     = "RV",
            currency     = "VND",
            posting_date = posting_date,
            document_date= document_date,
            gl_lines     = gl_lines,
            reference    = f"SO-{random.randint(200000000, 299999999)}",  # Sales Order ref
            header_text  = f"Bán {product['name']} cho {customer['customer_name'][:40]}",
        )

        # AR record
        ar_record = {
            "txn_id":       header["txn_id"],
            "customer_id":  customer["customer_id"],
            "invoice_no":   invoice_no,
            "invoice_date": posting_date,
            "due_date":     due_date,
            "amount":       gross_total,
            "currency":     "VND",
            "amount_vnd":   gross_total,
            "paid_amount":  0,
            "status":       "OPEN",
            "payment_method": None,
            "sales_channel":  channel,
            "plant":          plant_id,
        }

        return {
            "header":    header,
            "gl_lines":  gl_lines,
            "ar_record": ar_record,
        }

    # ─────────────────────────────────────────────────────
    # EXPORT SALES (RV document in USD/EUR/JPY)
    # ─────────────────────────────────────────────────────
    def _generate_export_invoice(self, posting_date: date, document_date: date) -> dict:
        export_customers = [c for c in self.customers if c.get("customer_type") == "EXPORT"]
        if not export_customers:
            # Fallback to any customer if no export customers loaded yet
            export_customers = self.customers[:2]

        customer = random.choice(export_customers)
        currency = customer.get("currency", "USD")
        fx_rate  = self.fx_rates.get(currency, DEFAULT_FX_RATES.get(currency, 25000))

        # Export invoice amount in foreign currency
        usd_amount   = round(random.uniform(15000, 120000), 2)
        vnd_equivalent = round(usd_amount * fx_rate, -3)

        # VAT = 0% for exports (theo Thông tư 219)
        # Still post the GL line with amount 0 to show VAT code
        vat_amount_usd = 0.00

        # Cost center: Export sales dept
        cc_exp = next((cc for cc in self.cost_centers if "EXP" in cc.get("cost_center_id", "")), None)
        cost_center = cc_exp["cost_center_id"] if cc_exp else None

        plant_info = self.pick_plant()
        plant_id = plant_info["plant_id"] if plant_info else None

        gl_lines = [
            # Line 001: DEBIT 131 — AR Export Customer (in USD)
            self.make_gl_line(
                line_item=1, account_id="131", debit_credit="D",
                amount=usd_amount, amount_vnd=vnd_equivalent,
                customer_id=customer["customer_id"],
                item_text=f"Export AR - {customer['customer_name'][:40]}",
                tax_code="E",  # E = Exempt
            ),
            # Line 002: CREDIT 5121 — Export Revenue (in USD)
            self.make_gl_line(
                line_item=2, account_id="5121", debit_credit="C",
                amount=usd_amount, amount_vnd=vnd_equivalent,
                cost_center=cost_center,
                plant=plant_id,
                item_text=f"Export sale - {customer['customer_name'][:30]}",
                tax_code="E",
                profit_center="PC-EXPORT",
            ),
        ]

        assert self.validate_double_entry(gl_lines)

        invoice_no = self.make_invoice_number("EXP", self.generate_doc_number("RV"))
        due_date   = self.get_due_date(posting_date, customer.get("payment_terms", "NET60"))

        header = self.build_header(
            doc_type      = "RV",
            currency      = currency,
            posting_date  = posting_date,
            document_date = document_date,
            gl_lines      = gl_lines,
            reference     = f"EXP-{random.randint(10000000, 19999999)}",
            header_text   = f"Export {currency} - {customer['customer_name'][:40]}",
            exchange_rate = fx_rate,
        )

        ar_record = {
            "txn_id":       header["txn_id"],
            "customer_id":  customer["customer_id"],
            "invoice_no":   invoice_no,
            "invoice_date": posting_date,
            "due_date":     due_date,
            "amount":       usd_amount,
            "currency":     currency,
            "amount_vnd":   vnd_equivalent,
            "paid_amount":  0,
            "status":       "OPEN",
            "payment_method": None,
            "sales_channel":  "EXPORT",
            "plant":          plant_id,
        }

        return {
            "header":    header,
            "gl_lines":  gl_lines,
            "ar_record": ar_record,
        }
