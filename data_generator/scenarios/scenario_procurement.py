"""
VinaMilk ERP — Procurement Scenario (Mua hàng / Nhập nguyên vật liệu)
=======================================================================
Simulates VinaMilk's procurement process:

  RAW_MATERIAL: Mua sữa tươi, đường, phụ gia thực phẩm
    - Document type: KR (Vendor Invoice - hóa đơn từ NCC)
    - Debit:  152  (Nguyên vật liệu)         net amount
    - Debit:  1331 (VAT Input 10%)            10% of net
    - Credit: 331  (Phải trả NCC)            total incl. VAT

  PACKAGING: Mua bao bì, hộp, chai
    - Same accounting as RAW_MATERIAL
    - Different vendor pool, different amounts

  SERVICE: Mua dịch vụ (điện, vận chuyển, bảo trì)
    - Debit:  641 / 627 / 642  (Expense account)    net
    - Debit:  1331 (VAT Input)                       10%
    - Credit: 331  (AP)                              total

VAS Rule: Input VAT (GTGT đầu vào) is deductible against output VAT.
"""

import random
from datetime import date, timedelta
from scenarios.base_scenario import BaseScenario


class ProcurementScenario(BaseScenario):
    """Generates vendor invoice documents (KR) for VinaMilk procurement."""

    VAT_INPUT_RATE = 0.10  # 10% input VAT on purchases

    # Amount ranges by category (VND)
    AMOUNT_RANGES = {
        "RAW_MATERIAL": {
            "VEND-RM001": (800_000_000,  5_000_000_000),   # Mộc Châu: large volumes
            "VEND-RM002": (500_000_000,  3_000_000_000),   # Ba Vì farms
            "VEND-RM003": (200_000_000,  1_500_000_000),   # Sugar (Quảng Ngãi)
            "default":    (100_000_000,  800_000_000),
        },
        "PACKAGING": {
            "VEND-PK001": (300_000_000,  3_000_000_000),   # Tetra Pak (high value)
            "default":    (50_000_000,   500_000_000),
        },
        "SERVICE": {
            "VEND-SV001": (500_000_000,  2_000_000_000),   # EVN electricity (big plant)
            "VEND-SV003": (50_000_000,   300_000_000),     # Advertising
            "default":    (20_000_000,   200_000_000),
        },
        "LOGISTICS": {
            "default":    (30_000_000,   400_000_000),
        },
        "EQUIPMENT": {
            "default":    (500_000_000,  10_000_000_000),  # Capital expenditure
        },
    }

    # Service expense accounts by vendor type
    SERVICE_EXPENSE_ACCOUNTS = {
        "VEND-SV001": "6272",   # Electricity → Chi phí điện nước nhà máy
        "VEND-SV002": "6272",   # Water
        "VEND-SV003": "6412",   # Advertising → Chi phí marketing
        "VEND-SV004": "642",    # Audit → Chi phí QLDN
        "VEND-LG001": "6413",   # GHN logistics → Chi phí vận chuyển
        "VEND-LG002": "6413",   # Viettel Post
        "VEND-LG003": "6413",   # Kuehne+Nagel
        "VEND-EQ001": "6271",   # GEA equipment maintenance
        "VEND-EQ002": "627",    # Samsung maintenance
        "VEND-EQ003": "627",    # Siemens
        "default":    "642",    # General QLDN
    }

    def __init__(self, master_data: dict, category: str = "RAW_MATERIAL"):
        super().__init__(master_data)
        self.category = category

    def generate(self) -> dict:
        posting_date  = self.get_posting_date(max_days_back=7)  # AP can be older
        document_date = posting_date - timedelta(days=random.randint(1, 5))  # Vendor invoice date

        if self.category in ("RAW_MATERIAL", "PACKAGING"):
            return self._generate_material_purchase(posting_date, document_date)
        elif self.category in ("SERVICE", "LOGISTICS", "EQUIPMENT"):
            return self._generate_service_purchase(posting_date, document_date)
        else:
            return self._generate_material_purchase(posting_date, document_date)

    # ─────────────────────────────────────────────────────
    # MATERIAL PURCHASE (NVL / Bao bì)
    # ─────────────────────────────────────────────────────
    def _generate_material_purchase(self, posting_date: date, document_date: date) -> dict:
        vendor = self.pick_vendor(self.category)
        vendor_id = vendor["vendor_id"]
        currency  = vendor.get("currency", "VND")

        # Determine amount range for this vendor
        cat_ranges = self.AMOUNT_RANGES.get(self.category, {})
        min_amt, max_amt = cat_ranges.get(vendor_id, cat_ranges.get("default", (50_000_000, 500_000_000)))

        if currency == "USD":
            # Convert: get USD amount then VND equivalent
            usd_amount = round(random.uniform(min_amt / 25000, max_amt / 25000), 2)
            fx_rate    = self.fx_rates.get("USD", 25150)
            net_amount = usd_amount
            net_amount_vnd = round(usd_amount * fx_rate, -3)
        else:
            net_amount     = round(random.uniform(min_amt, max_amt), -3)
            net_amount_vnd = net_amount
            usd_amount     = None
            fx_rate        = 1.0

        vat_amount = round(net_amount * self.VAT_INPUT_RATE, -3 if currency == "VND" else 2)
        total_ap   = round(net_amount + vat_amount, 2)

        # NVL account: 152 for raw material, 152 for packaging too (or 156)
        nvl_account = "152" if self.category == "RAW_MATERIAL" else "152"

        # Cost center: production department
        plant_info  = self.pick_plant()
        plant_id    = plant_info["plant_id"] if plant_info else "VM01"
        prod_cc     = next(
            (cc for cc in self.cost_centers if "PRD" in cc.get("cost_center_id", "")),
            None
        )
        cost_center = prod_cc["cost_center_id"] if prod_cc else None

        # Purchase Order reference (SAP PO)
        po_number = self.make_po_number()

        gl_lines = [
            # Line 001: DEBIT 152 — Nhập kho nguyên vật liệu
            self.make_gl_line(
                line_item=1, account_id=nvl_account, debit_credit="D",
                amount=net_amount, amount_vnd=net_amount_vnd,
                cost_center=cost_center,
                plant=plant_id,
                vendor_id=vendor_id,
                item_text=f"Nhập {self.category} từ {vendor['vendor_name'][:40]}",
                assignment=po_number,
            ),
            # Line 002: DEBIT 1331 — VAT Input 10% (được khấu trừ)
            self.make_gl_line(
                line_item=2, account_id="1331", debit_credit="D",
                amount=vat_amount,
                item_text=f"GTGT đầu vào 10% - {vendor['vendor_name'][:30]}",
                tax_code="V1",
            ),
            # Line 003: CREDIT 331 — Phải trả NCC (full amount incl. VAT)
            self.make_gl_line(
                line_item=3, account_id="331", debit_credit="C",
                amount=total_ap, amount_vnd=round(total_ap * fx_rate, 2) if currency != "VND" else total_ap,
                vendor_id=vendor_id,
                item_text=f"AP - {vendor['vendor_name'][:40]}",
                assignment=vendor["vendor_id"],
                tax_code="V1",
            ),
        ]

        assert self.validate_double_entry(gl_lines), "Double-entry imbalance in procurement doc!"

        invoice_no = f"NCC-{vendor_id[-5:]}-{posting_date.strftime('%y%m')}-{random.randint(1000,9999)}"
        due_date   = self.get_due_date(document_date, vendor.get("payment_terms", "NET30"))

        header = self.build_header(
            doc_type      = "KR",
            currency      = currency,
            posting_date  = posting_date,
            document_date = document_date,
            gl_lines      = gl_lines,
            reference     = po_number,
            header_text   = f"Mua {self.category} - {vendor['vendor_name'][:40]}",
            exchange_rate = fx_rate,
        )

        ap_record = {
            "txn_id":        header["txn_id"],
            "vendor_id":     vendor_id,
            "invoice_no":    invoice_no,
            "invoice_date":  document_date,
            "due_date":      due_date,
            "amount":        total_ap,
            "currency":      currency,
            "amount_vnd":    round(total_ap * fx_rate, 2),
            "paid_amount":   0,
            "status":        "OPEN",
            "purchase_order": po_number,
            "vendor_type":   self.category,
            "plant":         plant_id,
        }

        return {
            "header":    header,
            "gl_lines":  gl_lines,
            "ap_record": ap_record,
        }

    # ─────────────────────────────────────────────────────
    # SERVICE PURCHASE (Điện, logistics, marketing, kiểm toán)
    # ─────────────────────────────────────────────────────
    def _generate_service_purchase(self, posting_date: date, document_date: date) -> dict:
        # Include logistics and equipment vendors too
        vendor_types = ["SERVICE", "LOGISTICS"]
        if random.random() < 0.1:  # 10% chance: equipment purchase
            vendor_types = ["EQUIPMENT"]
        vendor = self.pick_vendor(random.choice(vendor_types))
        vendor_id = vendor["vendor_id"]

        # Pick expense account for this vendor
        expense_account = self.SERVICE_EXPENSE_ACCOUNTS.get(
            vendor_id, self.SERVICE_EXPENSE_ACCOUNTS["default"]
        )

        # Amount range
        cat_ranges = self.AMOUNT_RANGES.get(vendor.get("vendor_type", "SERVICE"), {})
        min_amt, max_amt = cat_ranges.get(vendor_id, cat_ranges.get("default", (20_000_000, 200_000_000)))

        currency = vendor.get("currency", "VND")
        fx_rate  = self.fx_rates.get(currency, 1.0) if currency != "VND" else 1.0

        if currency != "VND":
            net_amount     = round(random.uniform(min_amt / 25000, max_amt / 25000), 2)
            net_amount_vnd = round(net_amount * fx_rate, -3)
        else:
            net_amount     = round(random.uniform(min_amt, max_amt), -3)
            net_amount_vnd = net_amount

        vat_amount = round(net_amount * self.VAT_INPUT_RATE, 2)
        total_ap   = round(net_amount + vat_amount, 2)

        # Cost center for service expense
        admin_cc = next(
            (cc for cc in self.cost_centers if "ADM" in cc.get("cost_center_id", "")),
            None
        )
        cost_center = admin_cc["cost_center_id"] if admin_cc else None

        gl_lines = [
            # Line 001: DEBIT — Expense account
            self.make_gl_line(
                line_item=1, account_id=expense_account, debit_credit="D",
                amount=net_amount, amount_vnd=net_amount_vnd,
                cost_center=cost_center,
                vendor_id=vendor_id,
                item_text=f"Chi phí {vendor['vendor_name'][:40]}",
                tax_code="V1",
            ),
            # Line 002: DEBIT 1331 — VAT Input
            self.make_gl_line(
                line_item=2, account_id="1331", debit_credit="D",
                amount=vat_amount,
                item_text="GTGT đầu vào 10%",
                tax_code="V1",
            ),
            # Line 003: CREDIT 331 — AP Vendor
            self.make_gl_line(
                line_item=3, account_id="331", debit_credit="C",
                amount=total_ap,
                vendor_id=vendor_id,
                item_text=f"AP - {vendor['vendor_name'][:40]}",
            ),
        ]

        assert self.validate_double_entry(gl_lines)

        invoice_no = f"SV-{vendor_id[-5:]}-{posting_date.strftime('%y%m')}-{random.randint(100,999)}"
        due_date   = self.get_due_date(document_date, vendor.get("payment_terms", "NET30"))

        header = self.build_header(
            doc_type      = "KR",
            currency      = currency,
            posting_date  = posting_date,
            document_date = document_date,
            gl_lines      = gl_lines,
            reference     = f"SV-{random.randint(1000000, 9999999)}",
            header_text   = f"Dịch vụ - {vendor['vendor_name'][:40]}",
            exchange_rate = fx_rate,
        )

        ap_record = {
            "txn_id":        header["txn_id"],
            "vendor_id":     vendor_id,
            "invoice_no":    invoice_no,
            "invoice_date":  document_date,
            "due_date":      due_date,
            "amount":        total_ap,
            "currency":      currency,
            "amount_vnd":    round(total_ap * fx_rate, 2),
            "paid_amount":   0,
            "status":        "OPEN",
            "purchase_order": None,
            "vendor_type":   vendor.get("vendor_type", "SERVICE"),
            "plant":         None,
        }

        return {
            "header":    header,
            "gl_lines":  gl_lines,
            "ap_record": ap_record,
        }
