"""
VinaMilk Data Lakehouse — Budget Plan Producer
===============================================
Giả lập Google Sheets Finance Planning cho phòng kế hoạch tài chính VinaMilk.

Real process:
  - Phòng KH-TC duy trì Google Sheets chứa ngân sách kế hoạch + forecast hàng tháng
  - Producer dùng gspread poll mỗi 5 phút, chỉ đẩy Kafka khi có thay đổi
  - Topic: erp.budget_plan  (retention 30 ngày)

Mock mode (không cần Google API):
  - Sinh dữ liệu ngân sách theo format thực tế
  - Poll mỗi 5 phút, mỗi lần random cập nhật 1-3 dòng (simulate user chỉnh số)

Run:
  python budget_plan_producer.py              # one-shot
  python budget_plan_producer.py --loop       # loop mỗi 5 phút
  python budget_plan_producer.py --backfill   # sinh đủ 12 tháng
"""

import os
import json
import time
import random
import logging
import argparse
import hashlib
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

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

# VinaMilk cost centers & channels
COST_CENTERS = [
    {"code": "CC-KD01", "name": "Kênh MT (Modern Trade)",       "division": "Sales"},
    {"code": "CC-KD02", "name": "Kênh GT (General Trade)",      "division": "Sales"},
    {"code": "CC-KD03", "name": "Kênh Export",                  "division": "Sales"},
    {"code": "CC-KD04", "name": "Kênh Online / E-commerce",     "division": "Sales"},
    {"code": "CC-MKT1", "name": "Marketing & Brand",            "division": "Marketing"},
    {"code": "CC-MKT2", "name": "Digital Marketing",            "division": "Marketing"},
    {"code": "CC-SX01", "name": "Nhà máy Thống Nhất",          "division": "Production"},
    {"code": "CC-SX02", "name": "Nhà máy Đà Lạt",              "division": "Production"},
    {"code": "CC-SX03", "name": "Nhà máy Tiên Sơn",            "division": "Production"},
    {"code": "CC-HC01", "name": "Hành chính - Nhân sự",         "division": "Admin"},
    {"code": "CC-HC02", "name": "Tài chính - Kế toán",          "division": "Finance"},
    {"code": "CC-HC03", "name": "Công nghệ thông tin",          "division": "IT"},
    {"code": "CC-RD01", "name": "Nghiên cứu & Phát triển",     "division": "R&D"},
    {"code": "CC-SC01", "name": "Chuỗi cung ứng - Logistics",  "division": "Supply Chain"},
    {"code": "CC-SC02", "name": "Procurement & Sourcing",       "division": "Supply Chain"},
    {"code": "CC-QA01", "name": "Kiểm soát chất lượng (QA)",   "division": "Quality"},
]

# Budget categories
BUDGET_CATEGORIES = {
    "Sales": [
        ("REVENUE",       "Doanh thu bán hàng",        5_000_000_000,  50_000_000_000),
        ("COGS",          "Giá vốn hàng bán",          3_000_000_000,  30_000_000_000),
        ("GROSS_PROFIT",  "Lợi nhuận gộp",             1_500_000_000,  20_000_000_000),
        ("TRADE_SPEND",   "Chi phí thương mại",          200_000_000,   2_000_000_000),
    ],
    "Marketing": [
        ("ATL_SPEND",     "Chi phí quảng cáo ATL",      500_000_000,   5_000_000_000),
        ("BTL_SPEND",     "Chi phí BTL / activation",   100_000_000,   1_000_000_000),
        ("DIGITAL_SPEND", "Chi phí digital",             50_000_000,     500_000_000),
    ],
    "Production": [
        ("RAW_MATERIAL",  "Chi phí nguyên vật liệu",   2_000_000_000,  20_000_000_000),
        ("LABOR",         "Chi phí nhân công SX",        500_000_000,   5_000_000_000),
        ("OVERHEAD",      "Chi phí sản xuất chung",      200_000_000,   2_000_000_000),
        ("MAINTENANCE",   "Chi phí bảo trì thiết bị",   100_000_000,   1_000_000_000),
    ],
    "Admin": [
        ("SALARY",        "Chi phí lương nhân viên",    200_000_000,   2_000_000_000),
        ("OFFICE",        "Chi phí văn phòng",           20_000_000,     200_000_000),
        ("TRAVEL",        "Chi phí công tác",            10_000_000,     100_000_000),
    ],
    "Finance": [
        ("INTEREST",      "Chi phí lãi vay",             50_000_000,     500_000_000),
        ("FOREX_RISK",    "Dự phòng rủi ro tỷ giá",     30_000_000,     300_000_000),
    ],
    "IT": [
        ("SAAS",          "Chi phí phần mềm SaaS",       50_000_000,     300_000_000),
        ("INFRA",         "Chi phí hạ tầng IT",          30_000_000,     200_000_000),
    ],
    "R&D": [
        ("RD_COST",       "Chi phí nghiên cứu phát triển", 100_000_000, 1_000_000_000),
    ],
    "Supply Chain": [
        ("LOGISTICS",     "Chi phí vận tải",            200_000_000,   2_000_000_000),
        ("WAREHOUSE",     "Chi phí kho bãi",            100_000_000,   1_000_000_000),
    ],
    "Quality": [
        ("QA_COST",       "Chi phí kiểm soát chất lượng", 50_000_000,   500_000_000),
    ],
}

# Track "last sheet state" để chỉ push khi có thay đổi
_last_hash: dict[str, str] = {}


# ─────────────────────────────────────────────────────────
# BUDGET DATA GENERATOR
# ─────────────────────────────────────────────────────────
def generate_budget_for_month(year: int, month: int) -> list[dict]:
    """Tạo kế hoạch ngân sách đầy đủ cho một tháng."""
    budget_records = []
    budget_date = date(year, month, 1)

    for cc in COST_CENTERS:
        division = cc["division"]
        categories = BUDGET_CATEGORIES.get(division, [])

        for cat_code, cat_name, min_amt, max_amt in categories:
            # Kế hoạch ban đầu (plan)
            planned = round(random.uniform(min_amt, max_amt), -6)

            # Forecast (có thể khác plan do điều chỉnh)
            variance_pct = random.uniform(-0.15, 0.20)   # ±15–20%
            forecast = round(planned * (1 + variance_pct), -6)

            # Actual YTD (chỉ có data cho các tháng đã qua)
            today = date.today()
            if budget_date < today.replace(day=1):
                actual_variance = random.uniform(-0.10, 0.12)
                actual = round(planned * (1 + actual_variance), -6)
            else:
                actual = None  # chưa có actual cho tháng tương lai

            record = {
                # Dimensions
                "budget_year":    year,
                "budget_month":   month,
                "budget_date":    budget_date.isoformat(),
                "cost_center":    cc["code"],
                "cost_center_name": cc["name"],
                "division":       division,
                "category_code":  cat_code,
                "category_name":  cat_name,
                "currency":       "VND",

                # Amounts
                "planned_amount": planned,
                "forecast_amount": forecast,
                "actual_amount":  actual,
                "variance_plan_forecast": round(forecast - planned, -3) if forecast else None,
                "variance_pct":   round(variance_pct * 100, 2),

                # Metadata
                "version":        1,
                "approved_by":    random.choice(["CFO", "Head of Finance", "Budget Committee"]),
                "last_updated_by": random.choice([
                    "nguyen.thi.a", "tran.van.b", "le.thi.c", "pham.van.d"
                ]),
                "sheet_name":     f"Budget_{year}_M{month:02d}",
                "source":         "GOOGLE_SHEETS_MOCK",
                "_ingested_at":   datetime.now().isoformat(),
                "_topic":         KAFKA_TOPIC,
            }
            budget_records.append(record)

    return budget_records


def simulate_sheet_update(records: list[dict]) -> list[dict]:
    """Giả lập user chỉnh 1-3 dòng trong sheet → tạo phiên bản mới."""
    updated = []
    n_updates = random.randint(1, 3)
    indices = random.sample(range(len(records)), min(n_updates, len(records)))

    for i, rec in enumerate(records):
        if i in indices:
            rec = dict(rec)  # copy
            # Điều chỉnh forecast ±5%
            old_forecast = rec["forecast_amount"]
            delta = random.uniform(-0.05, 0.05)
            rec["forecast_amount"] = round(old_forecast * (1 + delta), -6)
            rec["variance_plan_forecast"] = round(rec["forecast_amount"] - rec["planned_amount"], -3)
            rec["version"] = rec.get("version", 1) + 1
            rec["last_updated_by"] = random.choice([
                "nguyen.thi.a", "tran.van.b", "le.thi.c", "pham.van.d"
            ])
            rec["_ingested_at"] = datetime.now().isoformat()
            updated.append(rec)

    return updated


def compute_hash(records: list[dict]) -> str:
    """Hash nội dung sheet để detect thay đổi."""
    content = json.dumps(
        [(r["cost_center"], r["category_code"], r["forecast_amount"]) for r in records],
        sort_keys=True
    )
    return hashlib.md5(content.encode()).hexdigest()


# ─────────────────────────────────────────────────────────
# KAFKA PUBLISH
# ─────────────────────────────────────────────────────────
def publish_records(records: list[dict], producer) -> int:
    """Đẩy từng record vào Kafka topic."""
    count = 0
    for rec in records:
        key = f"{rec['budget_year']}-{rec['budget_month']:02d}_{rec['cost_center']}_{rec['category_code']}".encode()
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
def run_once(backfill: bool = False):
    """Một lần poll: sinh/cập nhật budget và push Kafka nếu có thay đổi."""
    today = date.today()

    # Tháng cần publish
    if backfill:
        months = [(today.year, m) for m in range(1, 13)]  # cả năm 2026
    else:
        # Current month + next 3 months (planning horizon)
        months = []
        for offset in range(0, 4):
            d = today + relativedelta(months=offset)
            months.append((d.year, d.month))

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
            logger.info(f"Connected to Kafka: {KAFKA_BOOTSTRAP}")
        except Exception as e:
            logger.error(f"Kafka connection failed: {e}")
            logger.info("Printing to console only")

    total_pushed = 0
    for year, month in months:
        sheet_key = f"{year}-{month:02d}"
        records = generate_budget_for_month(year, month)

        # Giả lập: sau lần đầu, chỉ update 1-3 dòng (người dùng chỉnh)
        if sheet_key in _last_hash:
            records_to_push = simulate_sheet_update(records)
            change_type = "UPDATE"
        else:
            records_to_push = records
            change_type = "FULL_SYNC"

        new_hash = compute_hash(records)

        # Chỉ push nếu có thay đổi (hash khác)
        if new_hash == _last_hash.get(sheet_key):
            logger.info(f"  {sheet_key}: No changes — skip")
            continue

        _last_hash[sheet_key] = new_hash

        if producer:
            pushed = publish_records(records_to_push, producer)
            total_pushed += pushed
            logger.info(
                f"  {sheet_key} [{change_type}]: pushed {pushed} records "
                f"({len(COST_CENTERS)} cost centers × categories)"
            )
        else:
            logger.info(f"  {sheet_key} [{change_type}]: {len(records_to_push)} records (no Kafka)")
            for r in records_to_push[:2]:
                logger.info(f"    Sample: {r['cost_center']} / {r['category_code']} = {r['planned_amount']:,.0f} VND")
            total_pushed += len(records_to_push)

    if producer:
        producer.close()

    logger.info(f"Budget poll complete. Total pushed: {total_pushed}")
    return total_pushed


def run_loop(interval: int = POLL_INTERVAL):
    """Loop mỗi 5 phút như Google Sheets webhook."""
    logger.info(f"Budget Plan Producer starting (poll every {interval}s / {interval//60} min)")
    while True:
        logger.info("Polling Google Sheets (mock)...")
        run_once()
        logger.info(f"Next poll in {interval}s")
        time.sleep(interval)


# ─────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="VinaMilk Budget Plan Producer")
    parser.add_argument("--loop",     action="store_true", help="Loop moi 5 phut (Loop every 5 minutes)")
    parser.add_argument("--backfill", action="store_true", help="Sinh du 12 thang nam 2026 (Backfill 12 months)")
    parser.add_argument("--interval", type=int, default=POLL_INTERVAL,
                        help=f"Poll interval (seconds, default: {POLL_INTERVAL})")
    args = parser.parse_args()

    if args.loop:
        run_loop(interval=args.interval)
    else:
        run_once(backfill=args.backfill)
