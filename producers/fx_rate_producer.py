"""
VinaMilk Data Lakehouse — FX Rate Producer
===========================================
Fetches live exchange rates VND/USD, VND/EUR, VND/JPY from ExchangeRate-API
and publishes to Kafka topic erp.fx_rates every hour.

Data flow:
  ExchangeRate-API → Python HTTP polling → Kafka topic erp.fx_rates
  (simulates VinaMilk Treasury team updating FX rates every hour from SBV)

Fallback: If API unavailable or no API key, uses realistic mock rates
with small random walks to simulate market movement.

Reference rate source: State Bank of Vietnam (SBV) - Ngân hàng Nhà nước
Real VinaMilk uses SBV daily reference rate + bank buying/selling spread.

Run:
  python fx_rate_producer.py                      # Run once
  python fx_rate_producer.py --loop               # Loop every hour
  python fx_rate_producer.py --mock               # Use mock data only
"""

import os
import json
import time
import random
import logging
import argparse
import requests
from datetime import datetime, date

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
logger = logging.getLogger("vinamik.fx_producer")

# ─────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP      = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC          = "erp.fx_rates"
API_KEY              = os.getenv("EXCHANGE_RATE_API_KEY", "")
API_URL              = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD"
POLL_INTERVAL        = int(os.getenv("FX_POLL_INTERVAL_SECONDS", "3600"))  # 1 hour

# Target currencies (VinaMilk's main FX exposures)
TARGET_CURRENCIES = ["USD", "EUR", "JPY", "SGD"]

# ─────────────────────────────────────────────────────────
# BASE RATES — SBV reference rates (approximate, updated periodically)
# These simulate the State Bank of Vietnam reference rates
# ─────────────────────────────────────────────────────────
SBV_BASE_RATES_VND = {
    "USD": 25_088.0,    # 1 USD = 25,088 VND (SBV reference)
    "EUR": 27_180.0,    # 1 EUR = 27,180 VND
    "JPY": 163.5,       # 1 JPY = 163.5 VND
    "SGD": 18_650.0,    # 1 SGD = 18,650 VND
    "VND": 1.0,
}

# Bank buying/selling spread (typical Vietnamese commercial bank)
BANK_SPREAD_PERCENT = {
    "USD": 0.005,   # ±0.5% spread
    "EUR": 0.008,   # ±0.8% spread
    "JPY": 0.006,   # ±0.6% spread
    "SGD": 0.010,   # ±1.0% spread
}

# Daily volatility (random walk)
DAILY_VOLATILITY = {
    "USD": 0.003,   # USD/VND relatively stable (SBV controlled)
    "EUR": 0.008,
    "JPY": 0.010,
    "SGD": 0.006,
}

# Track current rates for random walk
_current_rates = SBV_BASE_RATES_VND.copy()


# ─────────────────────────────────────────────────────────
# RATE FETCHING
# ─────────────────────────────────────────────────────────
def fetch_live_rates() -> dict:
    """Fetch live rates from ExchangeRate-API."""
    if not API_KEY:
        logger.warning("No API key — using mock rates")
        return generate_mock_rates()

    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("result") != "success":
            logger.error(f"API error: {data.get('error-type', 'unknown')}")
            return generate_mock_rates()

        # ExchangeRate-API returns rates relative to USD (base)
        usd_rates = data["conversion_rates"]

        # Convert to VND rates: VND per 1 foreign currency unit
        vnd_per_usd = usd_rates.get("VND", 25000)

        rates = {}
        for currency in TARGET_CURRENCIES:
            if currency == "USD":
                rates["USD"] = vnd_per_usd
            elif currency in usd_rates:
                # Rate: VND per 1 foreign currency
                # = (VND per USD) / (foreign per USD)
                rates[currency] = round(vnd_per_usd / usd_rates[currency], 2)

        rates["VND"] = 1.0
        logger.info(f"✅ Live FX rates fetched: USD={rates.get('USD', 0):,.1f} VND")
        return rates

    except requests.RequestException as e:
        logger.error(f"HTTP error fetching FX rates: {e}")
        return generate_mock_rates()


def generate_mock_rates() -> dict:
    """
    Generate realistic mock FX rates using random walk.
    Simulates SBV daily reference rate movements.
    """
    global _current_rates

    new_rates = {}
    for currency in TARGET_CURRENCIES:
        base_rate = _current_rates.get(currency, SBV_BASE_RATES_VND.get(currency, 1.0))
        volatility = DAILY_VOLATILITY.get(currency, 0.005)

        # Random walk: ±volatility% per hour
        change_pct = random.gauss(0, volatility / 8)  # Hourly fraction of daily vol
        new_rate = base_rate * (1 + change_pct)

        # Keep within ±5% of SBV base (SBV controlled corridor)
        sbv_base = SBV_BASE_RATES_VND[currency]
        new_rate = max(sbv_base * 0.95, min(sbv_base * 1.05, new_rate))
        new_rate = round(new_rate, 2)

        new_rates[currency] = new_rate
        _current_rates[currency] = new_rate

    new_rates["VND"] = 1.0
    logger.info(
        f"📊 Mock FX rates: "
        f"USD={new_rates['USD']:,.1f} | "
        f"EUR={new_rates['EUR']:,.1f} | "
        f"JPY={new_rates['JPY']:.2f} | "
        f"SGD={new_rates['SGD']:,.1f} VND"
    )
    return new_rates


# ─────────────────────────────────────────────────────────
# KAFKA PUBLISH
# ─────────────────────────────────────────────────────────
def publish_rates(rates: dict, producer) -> None:
    """Publish FX rate snapshot to Kafka."""
    timestamp = datetime.now()

    for currency, vnd_rate in rates.items():
        if currency == "VND":
            continue

        spread = BANK_SPREAD_PERCENT.get(currency, 0.005)

        message = {
            # Core rate data
            "currency_pair":     f"VND/{currency}",
            "base_currency":     "VND",
            "quote_currency":    currency,
            "vnd_per_unit":      vnd_rate,
            "rate_date":         timestamp.strftime("%Y-%m-%d"),
            "rate_time":         timestamp.strftime("%H:%M:%S"),
            "timestamp":         timestamp.isoformat(),

            # Bank buying/selling rates (for VinaMilk treasury)
            "bank_buying_rate":  round(vnd_rate * (1 - spread), 2),
            "bank_selling_rate": round(vnd_rate * (1 + spread), 2),

            # SBV reference rate context
            "sbv_reference":     SBV_BASE_RATES_VND.get(currency, vnd_rate),
            "deviation_pct":     round((vnd_rate / SBV_BASE_RATES_VND.get(currency, vnd_rate) - 1) * 100, 4),

            # Metadata
            "source":            "SBV_MOCK" if not API_KEY else "EXCHANGERATE_API",
            "fiscal_year":       timestamp.year,
            "fiscal_period":     timestamp.month,
        }

        key = f"FX_{currency}_{timestamp.strftime('%Y%m%d_%H')}".encode()
        producer.send(
            KAFKA_TOPIC,
            key=key,
            value=json.dumps(message, ensure_ascii=False).encode("utf-8")
        )
        logger.info(
            f"📤 Kafka → {KAFKA_TOPIC}: {message['currency_pair']} = "
            f"{vnd_rate:,.2f} VND (buy: {message['bank_buying_rate']:,.2f} / "
            f"sell: {message['bank_selling_rate']:,.2f})"
        )

    producer.flush()


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────
def run_once(use_mock: bool = False) -> dict:
    """Fetch and publish rates once."""
    rates = generate_mock_rates() if use_mock else fetch_live_rates()

    if not KAFKA_AVAILABLE:
        logger.warning("Kafka not available — printing rates only")
        for k, v in rates.items():
            if k != "VND":
                logger.info(f"  {k}: {v:,.2f} VND")
        return rates

    try:
        kafka_config = {
            "bootstrap_servers": KAFKA_BOOTSTRAP,
            "retries": 5,
            "request_timeout_ms": 10000,
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
        publish_rates(rates, producer)
        producer.close()
        logger.info(f"✅ FX rates published to Kafka '{KAFKA_TOPIC}'")
    except Exception as e:
        logger.error(f"❌ Kafka publish failed: {e}")

    return rates


def run_loop(use_mock: bool = False, interval: int = POLL_INTERVAL):
    """Run FX rate producer in continuous mode."""
    logger.info(f"⏰ FX Rate Producer starting — polling every {interval}s")

    while True:
        run_once(use_mock=use_mock)
        logger.info(f"💤 Next poll in {interval}s ({interval//60} minutes)")
        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="VinaMilk FX Rate Producer")
    parser.add_argument("--loop",  action="store_true", help="Run continuously every hour")
    parser.add_argument("--mock",  action="store_true", help="Use mock rates (no API key needed)")
    parser.add_argument("--interval", type=int, default=POLL_INTERVAL,
                        help=f"Polling interval in seconds (default: {POLL_INTERVAL})")
    args = parser.parse_args()

    if args.loop:
        run_loop(use_mock=args.mock, interval=args.interval)
    else:
        run_once(use_mock=args.mock)
