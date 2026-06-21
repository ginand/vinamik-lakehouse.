"""
VinaMilk ERP — Payroll & Depreciation & Finance Scenarios
===========================================================
Periodic (monthly) accounting entries:

PAYROLL (Bút toán lương - SA document):
  VinaMilk employs ~10,000 people. Monthly payroll ~450 billion VND.
  Each factory/department posts independently.
  - Debit:  622  (Direct labor cost - production workers)
  - Debit:  641x (Sales staff salary)
  - Debit:  642x (Management salary)
  - Debit:  3383 (Social Insurance - employer portion 17.5%)
  - Credit: 334  (Payable to employees)
  - Credit: 3383 (BHXH payable)

DEPRECIATION (Khấu hao TSCĐ - SA document):
  VinaMilk has 13 factories + equipment. Monthly depreciation ~80-120B VND.
  - Debit:  6271 (Depreciation expense - manufacturing)
  - Credit: 214  (Accumulated depreciation)

INTERCOMPANY (Nội bộ tập đoàn - SA document):
  Transfers between VinaMilk entities (1000 ↔ 1100 ↔ 1200).

BANK_CHARGES (Phí ngân hàng, lãi vay):
  - Debit:  635  (Interest expense / bank fees)
  - Credit: 112x (Bank debit)
"""

import random
from datetime import date, timedelta
from scenarios.base_scenario import BaseScenario


class PayrollDepreciationScenario(BaseScenario):
    """Generates periodic accounting entries: payroll, depreciation, interco, bank."""

    # Monthly payroll amounts by department type (VND)
    PAYROLL_AMOUNTS = {
        "PRODUCTION": (8_000_000_000,   20_000_000_000),  # Factory workers
        "SALES":      (2_000_000_000,    6_000_000_000),  # Sales team
        "ADMIN":      (1_500_000_000,    4_000_000_000),  # HQ management
        "LOGISTICS":  (500_000_000,      2_000_000_000),  # Logistics staff
        "RND":        (300_000_000,      800_000_000),    # R&D team
    }

    # Depreciation amounts by factory (VND/month)
    DEPRECIATION_AMOUNTS = {
        "VM01": (60_000_000_000,  120_000_000_000),   # Biggest factory
        "VM02": (50_000_000_000,  100_000_000_000),
        "VM03": (40_000_000_000,   80_000_000_000),
        "VM04": (30_000_000_000,   70_000_000_000),
        "VM05": (20_000_000_000,   50_000_000_000),
        "VM06": (25_000_000_000,   55_000_000_000),
        "VM07": (15_000_000_000,   35_000_000_000),
        "default": (5_000_000_000, 20_000_000_000),
    }

    def __init__(self, master_data: dict, mode: str = "PAYROLL"):
        super().__init__(master_data)
        self.mode = mode

    def generate(self) -> dict:
        if self.mode == "PAYROLL":
            return self._generate_payroll()
        elif self.mode == "DEPRECIATION":
            return self._generate_depreciation()
        elif self.mode == "INTERCOMPANY":
            return self._generate_intercompany()
        else:  # BANK_CHARGES
            return self._generate_bank_charges()

    # ─────────────────────────────────────────────────────
    # PAYROLL POSTING
    # Monthly: Kế toán tổng hợp post lương toàn công ty
    # ─────────────────────────────────────────────────────
    def _generate_payroll(self) -> dict:
        posting_date = date.today().replace(day=random.randint(25, 28))
        if posting_date > date.today():
            posting_date = date.today()

        # Pick a cost center (which department is this payroll for?)
        cc = self.pick_cost_center()
        cc_id   = cc["cost_center_id"] if cc else "ADM-HCM"
        cc_type = cc.get("cc_type", "ADMIN") if cc else "ADMIN"

        # Get amount range for this CC type
        min_amt, max_amt = self.PAYROLL_AMOUNTS.get(cc_type, self.PAYROLL_AMOUNTS["ADMIN"])
        gross_salary = round(random.uniform(min_amt, max_amt), -6)  # Round to million

        # BHXH employer portion: 17.5% of gross salary (VN law)
        bhxh_employer = round(gross_salary * 0.175, -6)
        # BHYT employer portion: 3%
        bhyt_employer = round(gross_salary * 0.03, -6)

        # Employee receives net: gross - deductions
        # We post employer's side only: expense = gross + employer social contributions
        employee_payable = round(gross_salary * 0.895, -6)  # Net approx (after 10.5% employee BHXH/BHYT)

        # Pick expense account based on CC type
        expense_accounts = {
            "PRODUCTION": ("622",  "Chi phí nhân công trực tiếp SX"),
            "SALES":      ("6411", "Chi phí nhân viên bán hàng"),
            "ADMIN":      ("6421", "Lương nhân viên QLDN"),
            "LOGISTICS":  ("641",  "Chi phí bán hàng - logistics"),
            "RND":        ("642",  "Chi phí QLDN - R&D"),
        }
        exp_acc, exp_text = expense_accounts.get(cc_type, ("642", "Chi phí lương"))

        gl_lines = [
            # Line 001: DEBIT — Salary expense (gross)
            self.make_gl_line(
                line_item=1, account_id=exp_acc, debit_credit="D",
                amount=gross_salary,
                cost_center=cc_id,
                item_text=f"{exp_text} tháng {posting_date.strftime('%m/%Y')}",
            ),
            # Line 002: DEBIT — BHXH employer contribution (17.5%)
            self.make_gl_line(
                line_item=2, account_id="3383", debit_credit="D",
                amount=bhxh_employer,
                cost_center=cc_id,
                item_text=f"BHXH chủ DN đóng - {cc_id}",
            ),
            # Line 003: CREDIT — Net salary payable to employees
            self.make_gl_line(
                line_item=3, account_id="334", debit_credit="C",
                amount=employee_payable,
                item_text=f"Phải trả lương NLĐ tháng {posting_date.strftime('%m/%Y')}",
            ),
            # Line 004: CREDIT — BHXH + BHYT payable (employer + employee)
            self.make_gl_line(
                line_item=4, account_id="3383", debit_credit="C",
                amount=round(gross_salary - employee_payable + bhxh_employer, -3),
                item_text="BHXH/BHYT phải nộp NHNN",
            ),
        ]

        assert self.validate_double_entry(gl_lines), f"Payroll double-entry failed! Debit={sum(l['amount'] for l in gl_lines if l['debit_credit']=='D'):.0f}, Credit={sum(l['amount'] for l in gl_lines if l['debit_credit']=='C'):.0f}"

        header = self.build_header(
            doc_type      = "SA",
            currency      = "VND",
            posting_date  = posting_date,
            document_date = posting_date,
            gl_lines      = gl_lines,
            reference     = f"PAYROLL-{cc_id}-{posting_date.strftime('%Y%m')}",
            header_text   = f"Bút toán lương {cc_id} T{posting_date.strftime('%m/%Y')}",
        )
        header["created_by"] = "BATCH_JOB"  # System automated posting

        return {"header": header, "gl_lines": gl_lines}

    # ─────────────────────────────────────────────────────
    # DEPRECIATION POSTING
    # Monthly: SAP depreciation run (automatic)
    # ─────────────────────────────────────────────────────
    def _generate_depreciation(self) -> dict:
        # Usually posted on last day of month
        today = date.today()
        last_day = today.replace(day=1) + timedelta(days=32)
        last_day = last_day.replace(day=1) - timedelta(days=1)
        posting_date = last_day if last_day <= today else today

        # Pick a factory
        plant_info = self.pick_plant()
        plant_id   = plant_info["plant_id"] if plant_info else "VM01"

        # Depreciation amount for this factory
        min_dep, max_dep = self.DEPRECIATION_AMOUNTS.get(plant_id, self.DEPRECIATION_AMOUNTS["default"])
        dep_amount = round(random.uniform(min_dep, max_dep), -6)

        # Production cost center for this plant
        prod_cc = next(
            (cc for cc in self.cost_centers
             if "PRD" in cc.get("cost_center_id", "") and cc.get("plant_id") == plant_id),
            None
        )
        cost_center = prod_cc["cost_center_id"] if prod_cc else "PRD-VM01"

        gl_lines = [
            # Line 001: DEBIT 6271 — Depreciation expense
            self.make_gl_line(
                line_item=1, account_id="6271", debit_credit="D",
                amount=dep_amount,
                cost_center=cost_center,
                plant=plant_id,
                item_text=f"Khấu hao TSCĐ {plant_id} T{posting_date.strftime('%m/%Y')}",
            ),
            # Line 002: CREDIT 214 — Accumulated depreciation
            self.make_gl_line(
                line_item=2, account_id="214", debit_credit="C",
                amount=dep_amount,
                plant=plant_id,
                item_text=f"Hao mòn lũy kế {plant_id}",
            ),
        ]

        assert self.validate_double_entry(gl_lines)

        header = self.build_header(
            doc_type      = "SA",
            currency      = "VND",
            posting_date  = posting_date,
            document_date = posting_date,
            gl_lines      = gl_lines,
            reference     = f"DEPR-{plant_id}-{posting_date.strftime('%Y%m')}",
            header_text   = f"Khấu hao tự động {plant_id} T{posting_date.strftime('%m/%Y')}",
        )
        header["created_by"] = "BATCH_JOB"

        return {"header": header, "gl_lines": gl_lines}

    # ─────────────────────────────────────────────────────
    # INTERCOMPANY TRANSFERS
    # ─────────────────────────────────────────────────────
    def _generate_intercompany(self) -> dict:
        posting_date = date.today()
        amount = round(random.uniform(500_000_000, 10_000_000_000), -6)

        gl_lines = [
            # Intercompany receivable (debit)
            self.make_gl_line(
                line_item=1, account_id="131", debit_credit="D",
                amount=amount,
                item_text="Phải thu nội bộ - VinaMilk Mộc Châu",
            ),
            # Bank outflow
            self.make_gl_line(
                line_item=2, account_id="1121", debit_credit="C",
                amount=amount,
                item_text="Chuyển tiền nội bộ tập đoàn",
            ),
        ]

        header = self.build_header(
            doc_type      = "SA",
            currency      = "VND",
            posting_date  = posting_date,
            document_date = posting_date,
            gl_lines      = gl_lines,
            header_text   = "Điều chuyển vốn nội bộ VinaMilk Group",
        )

        return {"header": header, "gl_lines": gl_lines}

    # ─────────────────────────────────────────────────────
    # BANK CHARGES & LOAN INTEREST
    # ─────────────────────────────────────────────────────
    def _generate_bank_charges(self) -> dict:
        posting_date = date.today()

        # Either bank fee or loan interest
        is_loan_interest = random.random() < 0.4

        if is_loan_interest:
            amount = round(random.uniform(50_000_000, 500_000_000), -6)  # Loan interest
            item_text = f"Lãi vay tháng {posting_date.strftime('%m/%Y')} - VCB"
        else:
            amount = round(random.uniform(1_000_000, 30_000_000), -3)  # Bank fee
            item_text = f"Phí dịch vụ ngân hàng {posting_date.strftime('%m/%Y')}"

        gl_lines = [
            # DEBIT 635 — Finance cost
            self.make_gl_line(
                line_item=1, account_id="635", debit_credit="D",
                amount=amount, item_text=item_text,
            ),
            # CREDIT 112x — Bank auto-debit
            self.make_gl_line(
                line_item=2, account_id="1121", debit_credit="C",
                amount=amount, item_text="VCB - auto debit",
            ),
        ]

        header = self.build_header(
            doc_type      = "SA",
            currency      = "VND",
            posting_date  = posting_date,
            document_date = posting_date,
            gl_lines      = gl_lines,
            reference     = f"BANK-{posting_date.strftime('%Y%m%d')}",
            header_text   = item_text,
        )

        return {"header": header, "gl_lines": gl_lines}
