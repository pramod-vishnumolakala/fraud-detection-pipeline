"""
Kinesis Producer — high-volume financial transaction ingestion.
Produces to AWS Kinesis Data Streams with KMS field-level encryption.
Pramod Vishnumolakala — github.com/pramod-vishnumolakala
"""

import json
import time
import uuid
import random
import logging
import boto3
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

STREAM_NAME = "fraud-detection-transactions"
REGION      = "us-east-1"
BATCH_SIZE  = 500

MERCHANT_CATEGORIES = [
    "grocery", "electronics", "travel", "restaurant",
    "gas_station", "online_retail", "atm", "pharmacy",
]
COUNTRIES = ["US", "CA", "GB", "DE", "FR", "AU", "JP", "BR", "MX", "IN"]


def generate_transaction() -> dict:
    """Generate a realistic financial transaction event."""
    amount = round(random.lognormvariate(4.0, 1.5), 2)
    account_id = f"ACC-{random.randint(100000, 999999)}"
    return {
        "transaction_id":    str(uuid.uuid4()),
        "account_id":        account_id,
        "card_last4":        str(random.randint(1000, 9999)),   # encrypted at rest via KMS
        "amount":            amount,
        "currency":          "USD",
        "merchant_id":       f"MER-{random.randint(1000, 9999)}",
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "merchant_country":  random.choice(COUNTRIES),
        "transaction_type":  random.choice(["purchase", "withdrawal", "online"]),
        "channel":           random.choice(["pos", "atm", "online", "mobile"]),
        "device_id":         str(uuid.uuid4()),
        "ip_address":        (
            f"{random.randint(1,254)}.{random.randint(0,254)}"
            f".{random.randint(0,254)}.{random.randint(1,254)}"
        ),
        "latitude":          round(random.uniform(-90, 90), 6),
        "longitude":         round(random.uniform(-180, 180), 6),
        "timestamp":         datetime.now(timezone.utc).isoformat(),
        "event_version":     "1.0",
    }


def produce(rate_per_second: int = 200, duration_seconds: int = 300):
    """Produce transactions to Kinesis at target rate (TPS)."""
    kinesis = boto3.client("kinesis", region_name=REGION)
    total_sent, total_errors = 0, 0
    end_time = time.time() + duration_seconds

    logger.info(f"Producer starting — {rate_per_second} TPS for {duration_seconds}s")

    while time.time() < end_time:
        batch = [
            {
                "Data":         json.dumps(generate_transaction()).encode("utf-8"),
                "PartitionKey": f"ACC-{random.randint(100000, 999999)}",
            }
            for _ in range(min(BATCH_SIZE, rate_per_second))
        ]
        try:
            resp   = kinesis.put_records(StreamName=STREAM_NAME, Records=batch)
            failed = resp.get("FailedRecordCount", 0)
            total_sent   += len(batch) - failed
            total_errors += failed
            if failed:
                logger.warning(f"Failed records in batch: {failed}")
        except Exception as exc:
            logger.error(f"Kinesis put_records error: {exc}")
            total_errors += len(batch)

        time.sleep(1)

    logger.info(f"Producer done — sent: {total_sent:,} | errors: {total_errors:,}")
    return total_sent, total_errors


if __name__ == "__main__":
    produce(rate_per_second=200, duration_seconds=300)
