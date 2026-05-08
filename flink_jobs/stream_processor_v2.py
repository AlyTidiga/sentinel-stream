"""
Sentinel Stream — Stream Processor v2
Règles vélocité + Scoring ML hybride
"""
import json
import time
import requests
from datetime import datetime, timedelta
from collections import defaultdict, deque
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER   = "localhost:9092"
TOPIC_INPUT    = "txn-raw"
TOPIC_ALERTS   = "fraud-alerts"
TOPIC_DLQ      = "txn-dead-letter"
CONSUMER_GROUP = "sentinel-stream-processor-v2"
ML_API_URL     = "http://localhost:8000/score"

card_windows = defaultdict(deque)

def clean_window(window, window_sec):
    cutoff = datetime.utcnow() - timedelta(seconds=window_sec)
    while window and window[0]["ts"] < cutoff:
        window.popleft()

def get_window_features(card_id, txn):
    """Calcule les features depuis les fenêtres glissantes"""
    window = card_windows[card_id]

    clean_window(window, 60)
    txn_count_60s = len(window)

    clean_window(window, 600)
    txn_count_10min  = len(window)
    amount_sum_10min = sum(e["amount"] for e in window)

    clean_window(window, 14400)
    countries = set(e["location"].split(",")[1] for e in window if "," in e["location"])
    current_country = txn.get("location","").split(",")[1] if "," in txn.get("location","") else ""
    if current_country:
        countries.add(current_country)
    distinct_countries = len(countries) if countries else 1

    ts = datetime.utcnow()
    return {
        "amount":                float(txn.get("amount", 0)),
        "hour":                  ts.hour,
        "txn_count_60s":         txn_count_60s,
        "txn_count_10min":       txn_count_10min,
        "amount_sum_10min":      round(amount_sum_10min, 2),
        "distinct_countries_4h": distinct_countries,
        "is_weekend":            1 if ts.weekday() >= 5 else 0,
        "is_foreign":            1 if current_country not in ["SN",""] else 0,
    }

def call_ml_api(features):
    """Appelle l'API FastAPI et retourne le score"""
    try:
        resp = requests.post(ML_API_URL, json=features, timeout=0.5)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"  [ML API] Erreur : {e}")
    return {"fraud_score": 0.0, "decision": "APPROVE", "latency_ms": 0}

def check_velocity_rules(card_id, features):
    """Règles déterministes sur les features calculées"""
    rules = []
    if features["txn_count_60s"] >= 3:
        rules.append("velocity_60s")
    if features["txn_count_10min"] >= 8:
        rules.append("velocity_10min")
    if features["amount_sum_10min"] >= 2000:
        rules.append("amount_10min")
    if features["distinct_countries_4h"] >= 2:
        rules.append("geo_impossibility")
    return rules

def build_alert(txn, features, ml_result, triggered_rules):
    score    = ml_result["fraud_score"]
    decision = ml_result["decision"]

    if len(triggered_rules) >= 2 or score >= 0.85:
        severity = "HIGH"
    elif triggered_rules or score >= 0.60:
        severity = "MEDIUM"
    else:
        severity = "LOW"

    return {
        "alert_id":        f"ALERT-{txn['transaction_id']}",
        "transaction_id":  txn["transaction_id"],
        "card_id":         txn["card_id"],
        "amount":          txn["amount"],
        "location":        txn.get("location",""),
        "merchant_id":     txn.get("merchant_id",""),
        "timestamp":       txn.get("timestamp",""),
        "alert_time":      datetime.utcnow().isoformat(),
        "triggered_rules": triggered_rules,
        "fraud_score":     score,
        "ml_decision":     decision,
        "ml_latency_ms":   ml_result["latency_ms"],
        "severity":        severity,
    }

def main():
    print("=" * 55)
    print("  SENTINEL STREAM — Processor v2 (Rules + ML)")
    print(f"  Input  : {TOPIC_INPUT}")
    print(f"  Alerts : {TOPIC_ALERTS}")
    print(f"  ML API : {ML_API_URL}")
    print("=" * 55)

    # Vérifie que l'API ML est disponible
    try:
        r = requests.get("http://localhost:8000/health", timeout=2)
        print(f"\n✅ ML API connectée : {r.json()}")
    except:
        print("\n⚠️  ML API non disponible — scoring désactivé")

    consumer = KafkaConsumer(
        TOPIC_INPUT,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        max_poll_records=50,
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
    )

    print("\nEn écoute sur txn-raw... (Ctrl+C pour arrêter)\n")

    txn_count   = 0
    alert_count = 0

    try:
        for msg in consumer:
            txn     = msg.value
            card_id = txn.get("card_id","unknown")
            txn_count += 1

            # 1. Calcule les features depuis les fenêtres
            features = get_window_features(card_id, txn)

            # 2. Appelle le modèle ML
            ml_result = call_ml_api(features)

            # 3. Évalue les règles déterministes
            triggered_rules = check_velocity_rules(card_id, features)

            # 4. Ajoute la transaction à la fenêtre
            card_windows[card_id].append({
                "ts":       datetime.utcnow(),
                "amount":   float(txn.get("amount", 0)),
                "location": txn.get("location","")
            })

            # 5. Décision hybride : règle OU score ML élevé
            should_alert = (
                len(triggered_rules) > 0 or
                ml_result["fraud_score"] >= 0.60
            )

            if should_alert:
                alert = build_alert(txn, features, ml_result, triggered_rules)
                producer.send(TOPIC_ALERTS, key=card_id, value=alert)
                alert_count += 1
                print(f"🚨 [{alert['severity']}] card={card_id} "
                      f"score={ml_result['fraud_score']:.2f} "
                      f"decision={ml_result['decision']} "
                      f"rules={triggered_rules} "
                      f"latency={ml_result['latency_ms']}ms")
            else:
                if txn_count % 25 == 0:
                    print(f"✅ {txn_count} txn — {alert_count} alertes "
                          f"| dernière : card={card_id} "
                          f"score={ml_result['fraud_score']:.2f}")

    except KeyboardInterrupt:
        print(f"\nArrêt — {txn_count} txn, {alert_count} alertes")
    finally:
        consumer.close()
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
