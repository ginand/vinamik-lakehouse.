"""
VinaMilk Data Lakehouse — MISA CSV Producer
============================================
Simulates the MISA SME software export process from VinaMilk's subsidiary
companies (Dalat Milk - bukrs 1100, Mộc Châu Milk - bukrs 1200).

Real process in VinaMilk:
  - Subsidiary companies use MISA SME accounting software
  - Every day at 6:00 AM, the system exports transactions to CSV
  - The CSV is uploaded to a shared folder / SFTP
  - This producer reads the CSV and pushes to Kafka topic erp.misa_invoices

MISA CSV Format (real MISA export):
  Số chứng từ, Ngày chứng từ, Ngày hạch toán, Diễn giải, Mã KH/NCC,
  Tên KH/NCC, Mã tài khoản Nợ, Mã tài khoản Có, Số tiền, Mã tiền tệ

Run:
  python misa_csv_producer.py                 # One-shot (for today's CSV)
  python misa_csv_producer.py --generate-only # Generate CSV only, no Kafka
  python misa_csv_producer.py --loop          # Keep running (daily batch mode)
"""

import os
import csv
import json
import time
import random
import logging
import argparse
import schedule
from datetime import datetime, date, timedelta
from typing import List, Dict

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logging.warning("kafka-python not installed — CSV will be generated but not sent to Kafka")

# ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("vinamik.misa_producer")

# Config
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC      = "erp.misa_invoices"
CSV_OUTPUT_DIR   = os.getenv("MISA_CSV_DIR", "./data/misa_exports")

# ─────────────────────────────────────────────────────────
# MISA SUBSIDIARY MASTER DATA
# ─────────────────────────────────────────────────────────
SUBSIDIARIES = {
    "1100": {
        "name": "Công ty TNHH Sữa Lâm Đồng (Dalat Milk)",
        "tax_code": "5800231461",
        "products": ["Sữa tươi Đà Lạt 1L", "Sữa tươi Dalat 0.5L", "Sữa chua Dalat"],
    },
    "1200": {
        "name": "Công ty CP Sữa Mộc Châu",
        "tax_code": "2700290013",
        "products": ["Sữa tươi Mộc Châu", "Sữa chua Mộc Châu", "Phô mai Mộc Châu"],
    }
}

MISA_CUSTOMERS = [
    {"code": "KH001", "name": "Siêu thị CoopMart Đà Lạt"},
    {"code": "KH002", "name": "Đại lý Minh Hùng - TP Đà Lạt"},
    {"code": "KH003", "name": "Siêu thị BigC Lâm Đồng"},
    {"code": "KH004", "name": "Nhà phân phối Mộc Châu 01"},
    {"code": "KH005", "name": "Đại lý Sơn La Milk"},
    {"code": "KH006", "name": "Siêu thị Vinmart Hòa Bình"},
    {"code": "KH007", "name": "Công ty TNHH TM Tây Bắc"},
]

MISA_VENDORS = [
    {"code": "NCC001", "name": "Trang trại bò sữa Mộc Châu", "type": "RAW_MATERIAL"},
    {"code": "NCC002", "name": "HTX Nông nghiệp Ba Vì", "type": "RAW_MATERIAL"},
    {"code": "NCC003", "name": "Điện lực Lâm Đồng (PC Lâm Đồng)", "type": "SERVICE"},
    {"code": "NCC004", "name": "Công ty vận tải Tây Nguyên", "type": "LOGISTICS"},
    {"code": "NCC005", "name": "Công ty bao bì Đông Nam", "type": "PACKAGING"},
]

# MISA-style account codes (MISA uses shorter codes)
MISA_ACCOUNTS = {
    "DR_SALES":   {"no": "1311", "name": "Phải thu khách hàng"},
    "CR_REVENUE": {"no": "5111", "name": "Doanh thu bán hàng"},
    "CR_VAT":     {"no": "33311", "name": "Thuế GTGT đầu ra"},
    "DR_NVL":     {"no": "1521", "name": "Nguyên vật liệu"},
    "DR_VAT_IN":  {"no": "13311", "name": "Thuế GTGT đầu vào"},
    "CR_AP":      {"no": "3311", "name": "Phải trả người bán"},
}


# ─────────────────────────────────────────────────────────
# CSV GENERATOR — Mimics MISA export format
# ─────────────────────────────────────────────────────────
def generate_misa_csv(subsidiary_code: str, target_date: date, num_records: int = None) -> str:
    """
    Generate a MISA-format CSV file for the given subsidiary and date.
    Returns the path to the generated CSV file.
    """
    sub = SUBSIDIARIES.get(subsidiary_code, SUBSIDIARIES["1100"])

    if num_records is None:
        num_records = random.randint(15, 45)  # Daily transaction count per subsidiary

    os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)
    filename = f"misa_{subsidiary_code}_{target_date.strftime('%Y%m%d')}.csv"
    filepath = os.path.join(CSV_OUTPUT_DIR, filename)

    rows = []
    doc_counter = random.randint(1001, 1099)  # MISA document numbers

    for i in range(num_records):
        doc_counter += 1
        is_sale = random.random() < 0.70  # 70% sales, 30% purchases

        doc_date    = target_date - timedelta(days=random.randint(0, 1))
        acc_date    = target_date  # Accounting date = today

        if is_sale:
            customer   = random.choice(MISA_CUSTOMERS)
            product    = random.choice(sub["products"])
            net_amount = round(random.uniform(5_000_000, 150_000_000), -3)
            vat_amount = round(net_amount * 0.10, -3)
            total      = net_amount + vat_amount

            rows.append({
                "so_chung_tu":         f"HD{doc_counter:04d}",
                "ngay_chung_tu":       doc_date.strftime("%d/%m/%Y"),
                "ngay_hach_toan":      acc_date.strftime("%d/%m/%Y"),
                "dien_giai":           f"Bán {product} cho {customer['name']}",
                "ma_kh_ncc":           customer["code"],
                "ten_kh_ncc":          customer["name"],
                "ma_tai_khoan_no":     MISA_ACCOUNTS["DR_SALES"]["no"],
                "ma_tai_khoan_co":     MISA_ACCOUNTS["CR_REVENUE"]["no"],
                "so_tien":             total,
                "thue_gtgt":           vat_amount,
                "ma_tien_te":          "VND",
                "ty_gia":              1,
                "loai_chung_tu":       "Hóa đơn bán hàng",
                "ma_cty":              subsidiary_code,
                "ten_cty":             sub["name"],
                "nguoi_lap":           f"KT{random.randint(1,5):02d}",
            })
        else:
            vendor     = random.choice(MISA_VENDORS)
            net_amount = round(random.uniform(10_000_000, 200_000_000), -3)
            vat_amount = round(net_amount * 0.10, -3)
            total      = net_amount + vat_amount

            rows.append({
                "so_chung_tu":         f"PC{doc_counter:04d}",
                "ngay_chung_tu":       doc_date.strftime("%d/%m/%Y"),
                "ngay_hach_toan":      acc_date.strftime("%d/%m/%Y"),
                "dien_giai":           f"Mua {vendor['type']} từ {vendor['name']}",
                "ma_kh_ncc":           vendor["code"],
                "ten_kh_ncc":          vendor["name"],
                "ma_tai_khoan_no":     MISA_ACCOUNTS["DR_NVL"]["no"],
                "ma_tai_khoan_co":     MISA_ACCOUNTS["CR_AP"]["no"],
                "so_tien":             total,
                "thue_gtgt":           vat_amount,
                "ma_tien_te":          "VND",
                "ty_gia":              1,
                "loai_chung_tu":       "Phiếu chi" if vendor["type"] == "SERVICE" else "Hóa đơn mua hàng",
                "ma_cty":              subsidiary_code,
                "ten_cty":             sub["name"],
                "nguoi_lap":           f"KT{random.randint(1,5):02d}",
            })

    # Write CSV with Vietnamese headers
    fieldnames = [
        "so_chung_tu", "ngay_chung_tu", "ngay_hach_toan", "dien_giai",
        "ma_kh_ncc", "ten_kh_ncc", "ma_tai_khoan_no", "ma_tai_khoan_co",
        "so_tien", "thue_gtgt", "ma_tien_te", "ty_gia",
        "loai_chung_tu", "ma_cty", "ten_cty", "nguoi_lap"
    ]

    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"✅ Generated MISA CSV: {filepath} ({len(rows)} records for {sub['name']})")
    return filepath


# ─────────────────────────────────────────────────────────
# KAFKA PRODUCER — Push CSV rows to Kafka
# ─────────────────────────────────────────────────────────
def push_csv_to_kafka(filepath: str, producer: "KafkaProducer") -> int:
    """Read CSV file and push each row as JSON to Kafka."""
    count = 0

    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Enrich with metadata
            message = {
                **row,
                "_source":     "MISA_CSV",
                "_file":       os.path.basename(filepath),
                "_ingested_at": datetime.now().isoformat(),
                "_topic":       KAFKA_TOPIC,
            }

            # Convert numeric strings
            for num_col in ["so_tien", "thue_gtgt", "ty_gia"]:
                try:
                    message[num_col] = float(message[num_col])
                except (ValueError, TypeError):
                    pass

            key = f"{row.get('ma_cty', 'MISA')}_{row.get('so_chung_tu', '')}".encode()
            producer.send(
                KAFKA_TOPIC,
                key=key,
                value=json.dumps(message, ensure_ascii=False).encode("utf-8")
            )
            count += 1

    producer.flush()
    logger.info(f"📨 Pushed {count} MISA records to Kafka topic '{KAFKA_TOPIC}'")
    return count


# ─────────────────────────────────────────────────────────
# DAILY BATCH JOB
# ─────────────────────────────────────────────────────────
def run_daily_batch(generate_only: bool = False, target_date: date = None):
    """Run the daily MISA CSV export batch (simulates 6:00 AM job)."""
    today = target_date or date.today()
    logger.info(f"🕕 Running MISA daily batch for {today}")

    producer = None
    if KAFKA_AVAILABLE and not generate_only:
        try:
            # Cấu hình cơ bản
            kafka_config = {
                "bootstrap_servers": KAFKA_BOOTSTRAP,
                "retries": 5,
                "linger_ms": 10,
                "batch_size": 16384,
            }

            # Nếu kết nối đến Azure Event Hubs thì cần thêm SASL/SSL
            if "servicebus.windows.net" in KAFKA_BOOTSTRAP:
                eh_conn_str = os.getenv("EVENT_HUBS_CONNECTION_STRING")
                if not eh_conn_str:
                    logger.error("❌ Thiếu EVENT_HUBS_CONNECTION_STRING trong biến môi trường")
                    raise ValueError("Thiếu chuỗi kết nối Event Hubs")
                
                kafka_config.update({
                    "security_protocol": "SASL_SSL",
                    "sasl_mechanism": "PLAIN",
                    "sasl_plain_username": "$ConnectionString",
                    "sasl_plain_password": eh_conn_str
                })

            producer = KafkaProducer(**kafka_config)
            logger.info(f"✅ Connected to Kafka: {KAFKA_BOOTSTRAP}")
        except Exception as e:
            logger.error(f"❌ Kafka connection failed: {e}")
            logger.info("⚠️ Falling back to generate-only mode")

    total_pushed = 0

    for sub_code in SUBSIDIARIES.keys():
        csv_path = generate_misa_csv(sub_code, today)

        if producer and not generate_only:
            pushed = push_csv_to_kafka(csv_path, producer)
            total_pushed += pushed

    if producer:
        producer.close()

    logger.info(f"🏁 Daily batch complete. Total records pushed: {total_pushed}")


# ─────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="VinaMilk MISA CSV Producer")
    parser.add_argument("--generate-only", action="store_true",
                        help="Generate CSVs without sending to Kafka")
    parser.add_argument("--loop", action="store_true",
                        help="Run on schedule (daily at 06:00)")
    parser.add_argument("--backfill", type=int, default=0,
                        help="Number of past days to backfill (e.g., 180 for 6 months)")
    args = parser.parse_args()

    if args.loop:
        logger.info("⏰ MISA producer in scheduled mode — runs daily at 06:00")
        # Run once immediately on start
        run_daily_batch(generate_only=args.generate_only)
        schedule.every().day.at("06:00").do(run_daily_batch, generate_only=args.generate_only)
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        if args.backfill > 0:
            logger.info(f"⏪ Starting backfill for the last {args.backfill} days...")
            for i in range(args.backfill, -1, -1):
                past_date = date.today() - timedelta(days=i)
                run_daily_batch(generate_only=args.generate_only, target_date=past_date)
        else:
            run_daily_batch(generate_only=args.generate_only)
