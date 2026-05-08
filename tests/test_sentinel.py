import sys
import pytest
from datetime import datetime, timedelta
from collections import deque

sys.path.insert(0, '.')
from flink_jobs.stream_processor_v2 import (
    check_velocity_rules,
    get_window_features,
    build_alert,
    card_windows
)

# ─── FIXTURES ─────────────────────────────────────────────────
def make_txn(card_id="CARD-TEST", amount=100.0, location="Dakar,SN"):
    return {
        "transaction_id": "TXN-001",
        "card_id":        card_id,
        "amount":         amount,
        "location":       location,
        "merchant_id":    "MERCHANT-001",
        "timestamp":      datetime.utcnow().isoformat()
    }

def make_features(txn_count_60s=1, txn_count_10min=1,
                  amount_sum_10min=100.0, distinct_countries=1,
                  amount=100.0):
    return {
        "amount":                amount,
        "hour":                  14,
        "txn_count_60s":         txn_count_60s,
        "txn_count_10min":       txn_count_10min,
        "amount_sum_10min":      amount_sum_10min,
        "distinct_countries_4h": distinct_countries,
        "is_weekend":            0,
        "is_foreign":            0,
    }

# ─── TESTS RÈGLES DE VÉLOCITÉ ─────────────────────────────────
class TestVelocityRules:

    def test_no_alert_normal_transaction(self):
        features = make_features(txn_count_60s=1, txn_count_10min=2)
        rules = check_velocity_rules("CARD-001", features)
        assert rules == [], f"Aucune règle attendue, got {rules}"

    def test_velocity_60s_triggered(self):
        features = make_features(txn_count_60s=4)
        rules = check_velocity_rules("CARD-001", features)
        assert "velocity_60s" in rules

    def test_velocity_60s_not_triggered_at_threshold(self):
        features = make_features(txn_count_60s=2)
        rules = check_velocity_rules("CARD-001", features)
        assert "velocity_60s" not in rules

    def test_velocity_10min_triggered(self):
        features = make_features(txn_count_10min=9)
        rules = check_velocity_rules("CARD-001", features)
        assert "velocity_10min" in rules

    def test_amount_10min_triggered(self):
        features = make_features(amount_sum_10min=2500.0)
        rules = check_velocity_rules("CARD-001", features)
        assert "amount_10min" in rules

    def test_amount_10min_not_triggered(self):
        features = make_features(amount_sum_10min=500.0)
        rules = check_velocity_rules("CARD-001", features)
        assert "amount_10min" not in rules

    def test_geo_impossibility_triggered(self):
        features = make_features(distinct_countries=2)
        rules = check_velocity_rules("CARD-001", features)
        assert "geo_impossibility" in rules

    def test_multiple_rules_triggered(self):
        features = make_features(
            txn_count_60s=5,
            txn_count_10min=10,
            amount_sum_10min=3000.0,
            distinct_countries=3
        )
        rules = check_velocity_rules("CARD-001", features)
        assert len(rules) == 4

    def test_fraud_card_burst(self):
        features = make_features(
            txn_count_60s=8,
            txn_count_10min=15,
            amount_sum_10min=500.0,
            distinct_countries=1
        )
        rules = check_velocity_rules("FRAUD-001", features)
        assert "velocity_60s" in rules
        assert "velocity_10min" in rules

# ─── TESTS BUILD ALERT ────────────────────────────────────────
class TestBuildAlert:

    def test_high_severity_multiple_rules(self):
        txn      = make_txn()
        features = make_features()
        ml_result = {"fraud_score": 0.95, "decision": "BLOCK", "latency_ms": 5.0}
        rules    = ["velocity_60s", "geo_impossibility"]
        alert    = build_alert(txn, features, ml_result, rules)
        assert alert["severity"] == "HIGH"
        assert alert["ml_decision"] == "BLOCK"
        assert alert["fraud_score"] == 0.95

    def test_medium_severity_single_rule(self):
        txn      = make_txn()
        features = make_features()
        ml_result = {"fraud_score": 0.30, "decision": "APPROVE", "latency_ms": 3.0}
        rules    = ["velocity_60s"]
        alert    = build_alert(txn, features, ml_result, rules)
        assert alert["severity"] == "MEDIUM"

    def test_alert_contains_required_fields(self):
        txn      = make_txn()
        features = make_features()
        ml_result = {"fraud_score": 0.90, "decision": "BLOCK", "latency_ms": 4.0}
        alert    = build_alert(txn, features, ml_result, ["velocity_60s"])
        required = ["alert_id", "transaction_id", "card_id",
                    "amount", "fraud_score", "severity", "triggered_rules"]
        for field in required:
            assert field in alert, f"Champ manquant : {field}"

    def test_alert_id_format(self):
        txn      = make_txn()
        features = make_features()
        ml_result = {"fraud_score": 0.90, "decision": "BLOCK", "latency_ms": 4.0}
        alert    = build_alert(txn, features, ml_result, ["velocity_60s"])
        assert alert["alert_id"].startswith("ALERT-")

# ─── TESTS API ────────────────────────────────────────────────
class TestMLApi:

    def test_api_health(self):
        import requests
        resp = requests.get("http://localhost:8000/health", timeout=2)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_api_score_fraud(self):
        import requests
        payload = {
            "amount": 5000, "hour": 3,
            "txn_count_60s": 8, "txn_count_10min": 15,
            "amount_sum_10min": 8000, "distinct_countries_4h": 3,
            "is_weekend": 1, "is_foreign": 1
        }
        resp = requests.post("http://localhost:8000/score", json=payload, timeout=2)
        assert resp.status_code == 200
        data = resp.json()
        assert data["fraud_score"] > 0.80
        assert data["decision"] == "BLOCK"

    def test_api_score_legit(self):
        import requests
        payload = {
            "amount": 45, "hour": 14,
            "txn_count_60s": 1, "txn_count_10min": 2,
            "amount_sum_10min": 90, "distinct_countries_4h": 1,
            "is_weekend": 0, "is_foreign": 0
        }
        resp = requests.post("http://localhost:8000/score", json=payload, timeout=2)
        assert resp.status_code == 200
        data = resp.json()
        assert data["fraud_score"] < 0.50
        assert data["decision"] == "APPROVE"

    def test_api_latency(self):
        import requests, time
        payload = {
            "amount": 100, "hour": 10,
            "txn_count_60s": 1, "txn_count_10min": 3,
            "amount_sum_10min": 300, "distinct_countries_4h": 1,
            "is_weekend": 0, "is_foreign": 0
        }
        start = time.time()
        requests.post("http://localhost:8000/score", json=payload, timeout=2)
        latency = (time.time() - start) * 1000
        assert latency < 200, f"Latence trop haute : {latency:.1f}ms"
