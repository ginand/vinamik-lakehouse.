"""
VinaMilk Data Lakehouse — Budget Plan Producer (Real API Integration)
=====================================================================
Lấy dữ liệu thật từ Google Sheets do phòng kế toán - tài chính nhập liệu.
Sử dụng Google Service Account (credentials.json) để đọc dữ liệu qua API.

Run:
  python budget_plan_producer.py              # one-shot
  python budget_plan_producer.py --loop       # loop mỗi 5 phút
"""

import os
import json
import time
import logging
import argparse
import hashlib
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("vinamik.budget_producer")

# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = "erp.budget_plan"
POLL_INTERVAL   = int(os.getenv("BUDGET_POLL_INTERVAL", "300"))  # 5 phút
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
CREDENTIALS_FILE = "credentials.json"

_last_hash = ""

# ─────────────────────────────────────────────────────────
# GOOGLE SHEETS API
# ─────────────────────────────────────────────────────────
def fetch_budget_from_gsheets() -> list[dict]:
    if not GOOGLE_SHEET_ID:
        logger.error("❌ GOOGLE_SHEET_ID is empty! Please configure it in .env")
        return []

    if not os.path.exists(CREDENTIALS_FILE):
        logger.error(f"❌ Cannot find {CREDENTIALS_FILE}. Cannot authenticate with Google Sheets.")
        return []

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)

    try:
        sheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet("Budget_Plan_2026")
        records = sheet.get_all_records()
        
        parsed_records = []
        for r in records:
            # Bỏ qua các dòng trống
            if not str(r.get("Budget_ID", "")).strip():
                continue
                
            budget_id = str(r.get("Budget_ID", "")).strip()
            fiscal_year = int(r.get("Fiscal_Year", datetime.today().year))
            month = int(r.get("Month", datetime.today().month))
            cost_center = str(r.get("Cost_Center", "")).strip()
            department = str(r.get("Department", "")).strip()
            account_code = str(r.get("Account_Code", "")).strip()
            account_name = str(r.get("Account_Name", "")).strip()
            product_group = str(r.get("Product_Group", "")).strip()
            
            # Clean budget amount (in case of commas or dots)
            budget_amount_raw = r.get("Budget_Amount", 0)
            if isinstance(budget_amount_raw, str):
                budget_amount_raw = budget_amount_raw.replace('.', '').replace(',', '')
                try:
                    budget_amount = float(budget_amount_raw)
                except ValueError:
                    budget_amount = 0.0
            else:
                budget_amount = float(budget_amount_raw)
                
            currency = str(r.get("Currency", "VND")).strip()
            approved_by = str(r.get("Approved_By", "")).strip()
            status = str(r.get("Status", "")).strip()
            updated_date = str(r.get("Updated_Date", "")).strip()

            record = {
                "budget_id": budget_id,
                "fiscal_year": fiscal_year,
                "month": month,
                "cost_center": cost_center,
                "department": department,
                "account_code": account_code,
                "account_name": account_name,
                "product_group": product_group,
                "budget_amount": budget_amount,
                "currency": currency,
                "approved_by": approved_by,
                "status": status,
                "updated_date": updated_date,

                # Metadata
                "version": 1,
                "sheet_id": GOOGLE_SHEET_ID,
                "source": "GOOGLE_SHEETS_REAL_API",
                "_ingested_at": datetime.now().isoformat(),
                "_topic": KAFKA_TOPIC,
            }
            parsed_records.append(record)
        return parsed_records
    except Exception as e:
        logger.error(f"Error reading Google Sheets API: {e}")
        return []

def compute_hash(records: list[dict]) -> str:
    """Hash nội dung data để detect thay đổi."""
    content = json.dumps(
        [(r["budget_id"], r["budget_amount"], r["status"]) for r in records],
        sort_keys=True
    )
    return hashlib.md5(content.encode()).hexdigest()

# ─────────────────────────────────────────────────────────
# KAFKA PUBLISH
# ─────────────────────────────────────────────────────────
def publish_records(records: list[dict], producer) -> int:
    count = 0
    for rec in records:
        key = rec['budget_id'].encode()
        producer.send(
            KAFKA_TOPIC,
            key=key,
            value=json.dumps(rec, ensure_ascii=False, default=str).encode("utf-8")
        )
        count += 1
    producer.flush()
    return count

# ─────────────────────────────────────────────────────────
# MAIN LOGIC
# ─────────────────────────────────────────────────────────
def run_once():
    global _last_hash
    logger.info("Fetching data from Google Sheets API...")
    records = fetch_budget_from_gsheets()
    
    if not records:
        logger.info("No records fetched.")
        return 0

    new_hash = compute_hash(records)
    
    if new_hash == _last_hash:
        logger.info(f"Google Sheet data no changes (hash matched) — skipped.")
        return 0

    _last_hash = new_hash
    
    producer = None
    if KAFKA_AVAILABLE:
        try:
            kafka_config = {
                "bootstrap_servers": KAFKA_BOOTSTRAP,
                "retries": 5,
                "linger_ms": 50,
                "batch_size": 65536,
            }

            if "servicebus.windows.net" in KAFKA_BOOTSTRAP:
                eh_conn_str = os.getenv("EVENT_HUBS_CONNECTION_STRING")
                kafka_config.update({
                    "security_protocol": "SASL_SSL",
                    "sasl_mechanism": "PLAIN",
                    "sasl_plain_username": "$ConnectionString",
                    "sasl_plain_password": eh_conn_str
                })

            producer = KafkaProducer(**kafka_config)
        except Exception as e:
            logger.error(f"Kafka connection failed: {e}")

    total_pushed = 0
    if producer:
        total_pushed = publish_records(records, producer)
        producer.close()
        logger.info(f"✅ Pushed {total_pushed} records to Kafka topic {KAFKA_TOPIC}")
    else:
        logger.info(f"Found {len(records)} records (no Kafka)")
        for r in records[:2]:
            logger.info(f"  Sample: {r['budget_id']} = {r['budget_amount']:,.0f} {r['currency']}")
        total_pushed = len(records)

    return total_pushed

def run_loop(interval: int = POLL_INTERVAL):
    logger.info(f"Budget Plan Producer starting (poll every {interval}s)")
    while True:
        run_once()
        time.sleep(interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", action="store_true")
    # Add backfill arg to match docker-compose
    parser.add_argument("--backfill", action="store_true")
    args = parser.parse_args()

    if args.loop:
        run_loop()
    else:
        run_once()
