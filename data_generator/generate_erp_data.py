"""
VinaMilk ERP Data Generator — Main Orchestrator
================================================
Simulates SAP S/4HANA accounting transactions for VinaMilk Corporation.

Architecture:
  - Selects business scenario based on weighted probability
  - Each scenario generates a complete accounting document (Header + GL Lines)
  - Optionally updates AR/AP tables for relevant document types
  - DQ Injector wraps every scenario to add realistic data errors (15-20%)
  - Inserts at 1-2 records/second → captured by Debezium CDC → Kafka

Run:
  python generate_erp_data.py --speed normal --mode continuous
  python generate_erp_data.py --speed fast   --count 500
  python generate_erp_data.py --speed slow   --dry-run
"""

import time
import random
import logging
import argparse
import signal
import sys
from datetime import datetime, date, timedelta
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import connection as PgConnection

# Local imports
sys.path.insert(0, ".")
from config import (
    DB_CONFIG, SCENARIO_WEIGHTS, SPEED_CONFIGS, DEFAULT_SPEED,
    LOG_LEVEL, LOG_FORMAT, LOG_DATE_FORMAT, FISCAL_YEAR, COMPANY_CODE
)
from scenarios.scenario_revenue import RevenueScenario
from scenarios.scenario_collections import CollectionScenario
from scenarios.scenario_procurement import ProcurementScenario
from scenarios.scenario_payment import PaymentScenario
from scenarios.scenario_payroll_depreciation import PayrollDepreciationScenario
from dq_injector import DQInjector

# ─────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
logger = logging.getLogger("vinamik.generator")

# ─────────────────────────────────────────────────────────
# GRACEFUL SHUTDOWN
# ─────────────────────────────────────────────────────────
_running = True

def handle_sigterm(sig, frame):
    global _running
    logger.info("Received SIGTERM/SIGINT — shutting down gracefully...")
    _running = False

signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT,  handle_sigterm)


# ─────────────────────────────────────────────────────────
# DATABASE CONNECTION WITH RETRY
# ─────────────────────────────────────────────────────────
def get_db_connection(retries: int = 10, delay: float = 5.0) -> PgConnection:
    """Connect to PostgreSQL with retry logic for Docker startup race condition."""
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = False
            logger.info(f"✅ Connected to PostgreSQL — {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"⏳ DB connection attempt {attempt}/{retries} failed: {e}")
            if attempt == retries:
                logger.error("❌ Could not connect to PostgreSQL. Is Docker running?")
                raise
            time.sleep(delay)


def load_master_data(conn: PgConnection) -> dict:
    """
    Load master data from database into memory.
    Used by scenario classes to pick valid customers, vendors, accounts, etc.
    """
    with conn.cursor() as cur:
        # Customers
        cur.execute("""
            SELECT customer_id, customer_name, customer_type, sales_region,
                   sales_channel, payment_terms, currency, credit_limit
            FROM customers WHERE is_active = TRUE
        """)
        customers = [dict(zip([c.name for c in cur.description], row))
                     for row in cur.fetchall()]

        # Vendors
        cur.execute("""
            SELECT vendor_id, vendor_name, vendor_type, payment_terms, currency
            FROM vendors WHERE is_active = TRUE
        """)
        vendors = [dict(zip([c.name for c in cur.description], row))
                   for row in cur.fetchall()]

        # Chart of accounts
        cur.execute("""
            SELECT account_id, account_name, account_type, normal_balance,
                   allows_cost_center, requires_partner, is_reconciliation
            FROM chart_of_accounts WHERE is_active = TRUE
        """)
        accounts = {row[0]: dict(zip([c.name for c in cur.description], row))
                    for row in cur.fetchall()}

        # Cost centers
        cur.execute("""
            SELECT cost_center_id, cost_center_name, plant_id, cc_type
            FROM cost_centers WHERE is_active = TRUE
        """)
        cost_centers = [dict(zip([c.name for c in cur.description], row))
                        for row in cur.fetchall()]

        # Plants
        cur.execute("SELECT plant_id, plant_name, region FROM plants WHERE is_active = TRUE")
        plants = [dict(zip([c.name for c in cur.description], row))
                  for row in cur.fetchall()]

        # Open AR items (for collection scenario)
        cur.execute("""
            SELECT ar_id, customer_id, invoice_no, amount, paid_amount, currency
            FROM accounts_receivable
            WHERE status IN ('OPEN', 'PARTIAL')
            ORDER BY due_date ASC
            LIMIT 500
        """)
        open_ar = [dict(zip([c.name for c in cur.description], row))
                   for row in cur.fetchall()]

        # Open AP items (for payment scenario)
        cur.execute("""
            SELECT ap_id, vendor_id, invoice_no, amount, paid_amount, currency
            FROM accounts_payable
            WHERE status IN ('OPEN', 'PARTIAL')
            ORDER BY due_date ASC
            LIMIT 500
        """)
        open_ap = [dict(zip([c.name for c in cur.description], row))
                   for row in cur.fetchall()]

    logger.info(
        f"📦 Master data loaded: {len(customers)} customers, {len(vendors)} vendors, "
        f"{len(accounts)} GL accounts, {len(cost_centers)} cost centers | "
        f"Open AR: {len(open_ar)}, Open AP: {len(open_ap)}"
    )

    return {
        "customers":    customers,
        "vendors":      vendors,
        "accounts":     accounts,
        "cost_centers": cost_centers,
        "plants":       plants,
        "open_ar":      open_ar,
        "open_ap":      open_ap,
    }


# ─────────────────────────────────────────────────────────
# SCENARIO SELECTOR
# ─────────────────────────────────────────────────────────
def pick_scenario(master_data: dict, dq: DQInjector) -> dict:
    """
    Weighted random selection of business scenario.
    Returns a complete accounting document dict ready for DB insert.
    """
    scenario_key = random.choices(
        list(SCENARIO_WEIGHTS.keys()),
        weights=list(SCENARIO_WEIGHTS.values()),
        k=1
    )[0]

    scenarios = {
        "revenue_domestic":    RevenueScenario(master_data, export=False),
        "revenue_export":      RevenueScenario(master_data, export=True),
        "ar_collection":       CollectionScenario(master_data),
        "procurement_nvl":     ProcurementScenario(master_data, category="RAW_MATERIAL"),
        "procurement_service": ProcurementScenario(master_data, category="SERVICE"),
        "ap_payment":          PaymentScenario(master_data),
        "payroll":             PayrollDepreciationScenario(master_data, mode="PAYROLL"),
        "depreciation":        PayrollDepreciationScenario(master_data, mode="DEPRECIATION"),
        "intercompany":        PayrollDepreciationScenario(master_data, mode="INTERCOMPANY"),
        "bank_charges":        PayrollDepreciationScenario(master_data, mode="BANK_CHARGES"),
    }

    scenario = scenarios[scenario_key]
    doc = scenario.generate()

    # Apply data quality errors (15-20% of documents)
    doc = dq.maybe_inject_error(doc)

    doc["_scenario"] = scenario_key
    return doc


# ─────────────────────────────────────────────────────────
# DATABASE INSERT
# ─────────────────────────────────────────────────────────
def insert_document(conn: PgConnection, doc: dict) -> bool:
    """
    Atomically insert one complete accounting document:
    1. INSERT INTO transactions (header)
    2. INSERT INTO general_ledger (line items)
    3. INSERT INTO accounts_receivable or accounts_payable (if applicable)
    """
    try:
        with conn.cursor() as cur:

            # ── 1. Insert transaction header ──────────────────────────
            cur.execute("""
                INSERT INTO transactions (
                    txn_id, doc_number, doc_type, company_code,
                    fiscal_year, fiscal_period, posting_date, document_date,
                    reference, header_text, currency, exchange_rate,
                    total_debit, total_credit, status, created_by, source_system
                ) VALUES (
                    %(txn_id)s, %(doc_number)s, %(doc_type)s, %(company_code)s,
                    %(fiscal_year)s, %(fiscal_period)s, %(posting_date)s, %(document_date)s,
                    %(reference)s, %(header_text)s, %(currency)s, %(exchange_rate)s,
                    %(total_debit)s, %(total_credit)s, %(status)s, %(created_by)s, %(source_system)s
                )
            """, doc["header"])

            # ── 2. Insert GL line items ───────────────────────────────
            if doc.get("gl_lines"):
                execute_values(cur, """
                    INSERT INTO general_ledger (
                        txn_id, line_item, account_id, debit_credit,
                        amount, amount_vnd, cost_center, plant,
                        customer_id, vendor_id, tax_code, assignment,
                        item_text, profit_center
                    ) VALUES %s
                """, [
                    (
                        line["txn_id"], line["line_item"], line["account_id"],
                        line["debit_credit"], line["amount"], line.get("amount_vnd"),
                        line.get("cost_center"), line.get("plant"),
                        line.get("customer_id"), line.get("vendor_id"),
                        line.get("tax_code"), line.get("assignment"),
                        line.get("item_text"), line.get("profit_center"),
                    )
                    for line in doc["gl_lines"]
                ])

            # ── 3. Insert AR record (Revenue invoices only) ───────────
            if doc.get("ar_record"):
                ar = doc["ar_record"]
                cur.execute("""
                    INSERT INTO accounts_receivable (
                        txn_id, customer_id, invoice_no, invoice_date, due_date,
                        amount, currency, amount_vnd, paid_amount,
                        status, payment_method, sales_channel, plant
                    ) VALUES (
                        %(txn_id)s, %(customer_id)s, %(invoice_no)s, %(invoice_date)s, %(due_date)s,
                        %(amount)s, %(currency)s, %(amount_vnd)s, %(paid_amount)s,
                        %(status)s, %(payment_method)s, %(sales_channel)s, %(plant)s
                    )
                    ON CONFLICT (invoice_no) DO NOTHING
                """, ar)

            # ── 4. Insert AP record (Procurement only) ────────────────
            if doc.get("ap_record"):
                ap = doc["ap_record"]
                cur.execute("""
                    INSERT INTO accounts_payable (
                        txn_id, vendor_id, invoice_no, invoice_date, due_date,
                        amount, currency, amount_vnd, paid_amount,
                        status, purchase_order, vendor_type, plant
                    ) VALUES (
                        %(txn_id)s, %(vendor_id)s, %(invoice_no)s, %(invoice_date)s, %(due_date)s,
                        %(amount)s, %(currency)s, %(amount_vnd)s, %(paid_amount)s,
                        %(status)s, %(purchase_order)s, %(vendor_type)s, %(plant)s
                    )
                    ON CONFLICT (invoice_no) DO NOTHING
                """, ap)

            # ── 5. Update AR when customer pays (Collection) ──────────
            if doc.get("ar_update"):
                upd = doc["ar_update"]
                cur.execute("""
                    UPDATE accounts_receivable
                    SET paid_amount = %(paid_amount)s,
                        status = %(status)s,
                        cleared_date = %(cleared_date)s,
                        updated_at = NOW()
                    WHERE ar_id = %(ar_id)s
                """, upd)

            # ── 6. Update AP when vendor is paid (Payment) ────────────
            if doc.get("ap_update"):
                upd = doc["ap_update"]
                cur.execute("""
                    UPDATE accounts_payable
                    SET paid_amount = %(paid_amount)s,
                        status = %(status)s,
                        cleared_date = %(cleared_date)s,
                        updated_at = NOW()
                    WHERE ap_id = %(ap_id)s
                """, upd)

        conn.commit()
        return True

    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"DB error inserting document {doc.get('header', {}).get('doc_number')}: {e}")
        return False
    except Exception as e:
        conn.rollback()
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return False


# ─────────────────────────────────────────────────────────
# STATISTICS TRACKER
# ─────────────────────────────────────────────────────────
class Stats:
    def __init__(self):
        self.total = 0
        self.success = 0
        self.errors_inserted = 0
        self.scenario_counts = {}
        self.start_time = datetime.now()

    def record(self, scenario: str, success: bool, has_dq_error: bool):
        self.total += 1
        if success:
            self.success += 1
        if has_dq_error:
            self.errors_inserted += 1
        self.scenario_counts[scenario] = self.scenario_counts.get(scenario, 0) + 1

    def log_summary(self):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = self.total / elapsed if elapsed > 0 else 0
        error_pct = (self.errors_inserted / self.total * 100) if self.total > 0 else 0
        logger.info(
            f"📊 STATS | Total: {self.total} | Success: {self.success} | "
            f"DQ Errors injected: {self.errors_inserted} ({error_pct:.1f}%) | "
            f"Rate: {rate:.2f} txn/s | Elapsed: {elapsed:.0f}s"
        )
        logger.info(f"   Scenarios: {dict(sorted(self.scenario_counts.items(), key=lambda x: -x[1]))}")


# ─────────────────────────────────────────────────────────
# MAIN GENERATOR LOOP
# ─────────────────────────────────────────────────────────
def run_generator(speed: str, count: Optional[int], dry_run: bool):
    global _running

    speed_cfg = SPEED_CONFIGS.get(speed, SPEED_CONFIGS[DEFAULT_SPEED])
    stats = Stats()

    logger.info("=" * 60)
    logger.info("🏭 VinaMilk ERP Data Generator Starting")
    logger.info(f"   Speed: {speed} | Count: {count or 'unlimited'} | Dry run: {dry_run}")
    logger.info(f"   Target: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    logger.info(f"   DQ Error Rate: ~{sum([v for v in __import__('config').DQ_ERROR_RATES.values()])*100:.0f}%")
    logger.info("=" * 60)

    conn = get_db_connection() if not dry_run else None

    try:
        # Load master data (customers, vendors, accounts, etc.)
        if not dry_run:
            master_data = load_master_data(conn)
        else:
            # Minimal mock master data for dry run
            from master_data.company_structure import PLANTS, COST_CENTERS
            from master_data.chart_of_accounts import CHART_OF_ACCOUNTS
            from master_data.customers import CUSTOMERS_DATA
            from master_data.vendors import VENDORS_DATA
            master_data = {
                "customers":    CUSTOMERS_DATA,
                "vendors":      VENDORS_DATA,
                "accounts":     CHART_OF_ACCOUNTS,
                "cost_centers": [{"cost_center_id": k, **v} for k, v in COST_CENTERS.items()],
                "plants":       [{"plant_id": k, **v} for k, v in PLANTS.items()],
                "open_ar":      [],
                "open_ap":      [],
            }

        dq = DQInjector()
        iteration = 0

        while _running and (count is None or iteration < count):
            iteration += 1

            # Generate accounting document
            doc = pick_scenario(master_data, dq)
            scenario  = doc.get("_scenario", "unknown")
            has_error = doc.get("_dq_error_injected", False)

            if dry_run:
                # Just print — don't insert
                hdr = doc["header"]
                logger.info(
                    f"[DRY RUN] #{iteration:04d} | {hdr['doc_type']} {hdr['doc_number']} | "
                    f"{hdr['currency']} {hdr['total_debit']:>15,.0f} | "
                    f"Scenario: {scenario:<25} | DQ Error: {'✗ YES' if has_error else '✓ OK'}"
                )
                success = True
            else:
                success = insert_document(conn, doc)
                hdr = doc["header"]
                status_icon = "✅" if success else "❌"
                error_icon  = "⚠️ DQ" if has_error else "   "
                logger.info(
                    f"{status_icon} #{iteration:04d} | {hdr['doc_type']} {hdr['doc_number']} | "
                    f"{hdr['currency']} {hdr['total_debit']:>15,.0f} | "
                    f"{scenario:<25} {error_icon}"
                )

            stats.record(scenario, success, has_error)

            # Log summary every 100 transactions
            if iteration % 100 == 0:
                stats.log_summary()
                # Refresh open AR/AP from DB for collection/payment scenarios
                if not dry_run:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT ar_id, customer_id, invoice_no, amount, paid_amount, currency
                            FROM accounts_receivable WHERE status IN ('OPEN','PARTIAL') LIMIT 500
                        """)
                        master_data["open_ar"] = [
                            dict(zip([c.name for c in cur.description], row))
                            for row in cur.fetchall()
                        ]
                        cur.execute("""
                            SELECT ap_id, vendor_id, invoice_no, amount, paid_amount, currency
                            FROM accounts_payable WHERE status IN ('OPEN','PARTIAL') LIMIT 500
                        """)
                        master_data["open_ap"] = [
                            dict(zip([c.name for c in cur.description], row))
                            for row in cur.fetchall()
                        ]

            # Sleep between transactions (simulates real ERP throughput)
            sleep_time = random.uniform(speed_cfg["min"], speed_cfg["max"])
            time.sleep(sleep_time)

    finally:
        stats.log_summary()
        if conn:
            conn.close()
            logger.info("🔌 Database connection closed.")
        logger.info(f"🏁 Generator stopped after {stats.total} documents.")


# ─────────────────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(
        description="VinaMilk ERP Data Generator — SAP S/4HANA Mock"
    )
    parser.add_argument(
        "--speed",
        choices=["slow", "normal", "fast", "burst"],
        default=DEFAULT_SPEED,
        help="Generation speed (default: normal = 1-2 txn/s)"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Number of documents to generate (default: unlimited)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print generated documents without inserting to DB"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_generator(
        speed   = args.speed,
        count   = args.count,
        dry_run = args.dry_run,
    )
