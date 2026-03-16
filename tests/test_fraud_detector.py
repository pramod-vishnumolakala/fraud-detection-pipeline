"""
Unit tests for FraudDetector rule engine.
Pramod Vishnumolakala
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from src.processing.fraud_detector import FraudDetector, FRAUD_SCORE_THRESHOLD


def base_txn(**overrides):
    txn = {
        "transaction_id":    "txn-test-001",
        "account_id":        "ACC-111111",
        "amount":            50.0,
        "merchant_category": "grocery",
        "merchant_country":  "US",
        "channel":           "pos",
        "timestamp":         datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc).isoformat(),
    }
    txn.update(overrides)
    return txn


@pytest.fixture
def detector():
    with patch("src.processing.fraud_detector.boto3"):
        d = FraudDetector()
        d.alerts_table = MagicMock()
        d.sns          = MagicMock()
        return d


class TestHighAmountRule:
    def test_clean_below_threshold(self, detector):
        result = detector.process(base_txn(amount=100.0))
        assert not any(s.rule == "HIGH_AMOUNT" for s in result.signals)

    def test_flagged_above_threshold(self, detector):
        result = detector.process(base_txn(amount=6000.0))
        assert any(s.rule == "HIGH_AMOUNT" for s in result.signals)

    def test_exact_threshold_not_flagged(self, detector):
        result = detector.process(base_txn(amount=5000.0))
        assert not any(s.rule == "HIGH_AMOUNT" for s in result.signals)


class TestHighRiskCountryRule:
    @pytest.mark.parametrize("country", ["BR", "MX", "IN", "RU", "NG"])
    def test_high_risk_countries_flagged(self, detector, country):
        result = detector.process(base_txn(merchant_country=country))
        assert any(s.rule == "HIGH_RISK_COUNTRY" for s in result.signals)

    def test_safe_country_not_flagged(self, detector):
        result = detector.process(base_txn(merchant_country="US"))
        assert not any(s.rule == "HIGH_RISK_COUNTRY" for s in result.signals)


class TestHighRiskCategoryRule:
    @pytest.mark.parametrize("category", ["atm", "online_retail", "travel"])
    def test_high_risk_categories_flagged(self, detector, category):
        result = detector.process(base_txn(merchant_category=category))
        assert any(s.rule == "HIGH_RISK_CATEGORY" for s in result.signals)

    def test_safe_category_not_flagged(self, detector):
        result = detector.process(base_txn(merchant_category="grocery"))
        assert not any(s.rule == "HIGH_RISK_CATEGORY" for s in result.signals)


class TestRoundAmountRule:
    def test_round_amount_flagged(self, detector):
        result = detector.process(base_txn(amount=500.0))
        assert any(s.rule == "ROUND_AMOUNT" for s in result.signals)

    def test_non_round_not_flagged(self, detector):
        result = detector.process(base_txn(amount=523.47))
        assert not any(s.rule == "ROUND_AMOUNT" for s in result.signals)

    def test_small_round_not_flagged(self, detector):
        result = detector.process(base_txn(amount=50.0))
        assert not any(s.rule == "ROUND_AMOUNT" for s in result.signals)


class TestOffHoursRule:
    def test_off_hours_flagged(self, detector):
        ts = datetime(2024, 6, 15, 2, 0, 0, tzinfo=timezone.utc).isoformat()
        result = detector.process(base_txn(timestamp=ts))
        assert any(s.rule == "OFF_HOURS" for s in result.signals)

    def test_business_hours_not_flagged(self, detector):
        ts = datetime(2024, 6, 15, 14, 0, 0, tzinfo=timezone.utc).isoformat()
        result = detector.process(base_txn(timestamp=ts))
        assert not any(s.rule == "OFF_HOURS" for s in result.signals)


class TestFraudScoring:
    def test_fraud_alert_published_above_threshold(self, detector):
        # high amount + high risk country + high risk category → score >= 60
        result = detector.process(base_txn(
            amount=6000.0,
            merchant_country="BR",
            merchant_category="atm",
        ))
        assert result.is_fraud
        assert result.total_score >= FRAUD_SCORE_THRESHOLD
        detector.alerts_table.put_item.assert_called_once()
        detector.sns.publish.assert_called_once()

    def test_clean_txn_no_alert(self, detector):
        result = detector.process(base_txn())
        assert not result.is_fraud
        detector.alerts_table.put_item.assert_not_called()

    def test_score_accumulates_correctly(self, detector):
        result = detector.process(base_txn(
            amount=6000.0,      # +35
            merchant_country="BR",  # +20
        ))
        assert result.total_score == 55
