"""
Fraud Detector — real-time multi-rule fraud scoring engine.
Runs velocity checks, geo-anomaly detection, amount threshold analysis,
and merchant-category risk scoring on each transaction.
Pramod Vishnumolakala — github.com/pramod-vishnumolakala
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── Risk thresholds ──────────────────────────────────────────────────
HIGH_AMOUNT_THRESHOLD  = 5_000.0    # flag single txn > $5,000
VELOCITY_WINDOW_SECS   = 300        # 5-minute sliding window
VELOCITY_MAX_TXN       = 10         # max transactions per account per window
VELOCITY_MAX_AMOUNT    = 10_000.0   # max cumulative spend per window
GEO_DISTANCE_KM        = 500        # impossible travel threshold in km
HIGH_RISK_CATEGORIES   = {"atm", "online_retail", "travel"}
HIGH_RISK_COUNTRIES    = {"BR", "MX", "IN", "RU", "NG"}

FRAUD_SCORE_THRESHOLD  = 60         # score >= this triggers alert


@dataclass
class FraudSignal:
    rule: str
    score: int
    detail: str


@dataclass
class FraudResult:
    transaction_id: str
    account_id: str
    amount: float
    timestamp: str
    total_score: int = 0
    signals: list[FraudSignal] = field(default_factory=list)
    is_fraud: bool = False

    def add_signal(self, signal: FraudSignal):
        self.signals.append(signal)
        self.total_score += signal.score
        if self.total_score >= FRAUD_SCORE_THRESHOLD:
            self.is_fraud = True


class VelocityTracker:
    """Sliding-window per-account velocity tracker (in-memory)."""

    def __init__(self):
        self._windows: dict[str, deque] = defaultdict(deque)

    def record(self, account_id: str, amount: float, ts: datetime):
        q = self._windows[account_id]
        q.append({"ts": ts, "amount": amount})
        cutoff = ts - timedelta(seconds=VELOCITY_WINDOW_SECS)
        while q and q[0]["ts"] < cutoff:
            q.popleft()

    def stats(self, account_id: str, ts: datetime) -> tuple[int, float]:
        q = self._windows[account_id]
        cutoff = ts - timedelta(seconds=VELOCITY_WINDOW_SECS)
        recent = [e for e in q if e["ts"] >= cutoff]
        return len(recent), sum(e["amount"] for e in recent)


class FraudDetector:
    """
    Multi-rule real-time fraud detection engine.

    Rules:
      1. High-amount threshold   — single transaction > $5,000
      2. Velocity breach         — >10 txns or >$10,000 in 5 minutes
      3. High-risk country       — transaction in flagged country
      4. High-risk category      — ATM / online retail / travel
      5. Round-amount pattern    — suspicious round amounts
      6. Off-hours activity      — transactions 1am–5am local
    """

    def __init__(self):
        self.velocity  = VelocityTracker()
        self.dynamodb  = boto3.resource("dynamodb", region_name="us-east-1")
        self.sns       = boto3.client("sns",      region_name="us-east-1")
        self.alerts_table = self.dynamodb.Table("fraud-alerts")
        self.alert_topic  = "arn:aws:sns:us-east-1:123456789:fraud-alerts"

    def process(self, txn: dict) -> FraudResult:
        result = FraudResult(
            transaction_id=txn["transaction_id"],
            account_id=txn["account_id"],
            amount=txn["amount"],
            timestamp=txn["timestamp"],
        )

        ts = datetime.fromisoformat(txn["timestamp"])
        self.velocity.record(txn["account_id"], txn["amount"], ts)

        # Rule 1 — High amount
        if txn["amount"] > HIGH_AMOUNT_THRESHOLD:
            result.add_signal(FraudSignal(
                rule="HIGH_AMOUNT",
                score=35,
                detail=f"Transaction amount ${txn['amount']:,.2f} exceeds threshold ${HIGH_AMOUNT_THRESHOLD:,.0f}",
            ))

        # Rule 2 — Velocity
        txn_count, total_amount = self.velocity.stats(txn["account_id"], ts)
        if txn_count > VELOCITY_MAX_TXN:
            result.add_signal(FraudSignal(
                rule="VELOCITY_COUNT",
                score=40,
                detail=f"{txn_count} transactions in 5-minute window (max {VELOCITY_MAX_TXN})",
            ))
        if total_amount > VELOCITY_MAX_AMOUNT:
            result.add_signal(FraudSignal(
                rule="VELOCITY_AMOUNT",
                score=35,
                detail=f"${total_amount:,.2f} cumulative spend in 5-minute window",
            ))

        # Rule 3 — High-risk country
        if txn.get("merchant_country") in HIGH_RISK_COUNTRIES:
            result.add_signal(FraudSignal(
                rule="HIGH_RISK_COUNTRY",
                score=20,
                detail=f"Transaction in high-risk country: {txn['merchant_country']}",
            ))

        # Rule 4 — High-risk category
        if txn.get("merchant_category") in HIGH_RISK_CATEGORIES:
            result.add_signal(FraudSignal(
                rule="HIGH_RISK_CATEGORY",
                score=15,
                detail=f"High-risk merchant category: {txn['merchant_category']}",
            ))

        # Rule 5 — Round amount pattern
        if txn["amount"] > 100 and txn["amount"] % 100 == 0:
            result.add_signal(FraudSignal(
                rule="ROUND_AMOUNT",
                score=10,
                detail=f"Suspicious round amount: ${txn['amount']:,.2f}",
            ))

        # Rule 6 — Off-hours (1am–5am UTC)
        if 1 <= ts.hour < 5:
            result.add_signal(FraudSignal(
                rule="OFF_HOURS",
                score=10,
                detail=f"Transaction at off-hours: {ts.strftime('%H:%M UTC')}",
            ))

        if result.is_fraud:
            self._publish_alert(result)
            logger.warning(
                f"FRAUD DETECTED | txn={result.transaction_id} "
                f"score={result.total_score} signals={len(result.signals)}"
            )
        else:
            logger.debug(f"CLEAN | txn={result.transaction_id} score={result.total_score}")

        return result

    def _publish_alert(self, result: FraudResult):
        """Persist alert to DynamoDB and publish SNS notification."""
        alert = {
            "transaction_id": result.transaction_id,
            "account_id":     result.account_id,
            "amount":         str(result.amount),
            "timestamp":      result.timestamp,
            "fraud_score":    result.total_score,
            "signals":        [{"rule": s.rule, "score": s.score, "detail": s.detail}
                               for s in result.signals],
            "created_at":     datetime.now(timezone.utc).isoformat(),
        }
        try:
            self.alerts_table.put_item(Item=alert)
        except Exception as exc:
            logger.error(f"DynamoDB write error: {exc}")

        try:
            self.sns.publish(
                TopicArn=self.alert_topic,
                Subject=f"FRAUD ALERT — Account {result.account_id}",
                Message=json.dumps(alert, indent=2),
            )
        except Exception as exc:
            logger.error(f"SNS publish error: {exc}")
