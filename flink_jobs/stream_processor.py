"""
Sentinel Stream — Stream Processor
Implémente les règles de vélocité avec fenêtres glissantes
Consomme txn-raw, détecte les fraudes, publie dans fraud-alerts
"""

import json
import time
from datetime import datetime, timedelta
from collections import defaultdict, deque
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER   = "localhost:9092"
TOPIC_INPUT    = "txn-raw"
TOPIC_ALERTS   = "fraud-alerts"
TOPIC_DLQ      = "txn-dead-letter"
CONSUMER_GROUP = "sentinel-stream-processor"

# ─── FENÊTRES GLISSANTES ──────────────────────────────────────
# card_id -> deque de (timestamp, amount, location)
card_windows = defaultdict(deque)

# ─── RÈGLES DE VÉLOCITÉ ───────────────────────────────────────
RULES = {
    "velocity_60s":  {"window_sec": 60,   "max_count": 3},
    "velocity_10min":{"window_sec": 600,  "max_count": 8},
    "amount_10min":  {"window_sec": 600,  "max_amount": 2000},
    "geo_4h":        {"window_sec": 14400,"max_countries": 1},
}

def clean_window(window, window_sec):
    """Supprime les entrées expirées de la fenêtre"""
    cutoff = datetime.utcnow() - timedelta(seconds=window_sec)
    while window and window[0]["ts"] < cutoff:
        window.popleft()

def check_velocity_60s(window, txn):
    """Règle 1 : plus de 3 transactions en 60 secondes"""
    clean_window(window, 60)
    if len(window) >= RULES["velocity_60s"]["max_count"]:
        return {
            "rule":    "velocity_60s",
            "details": f"{len(window)} transactions en 60s (max={RULES['velocity_60s']['max_count']})",
            "count":   len(window)
        }
    return None

def check_velocity_10min(window, txn):
    """Règle 2 : plus de 8 transactions en 10 minutes"""
    clean_window(window, 600)
    if len(window) >= RULES["velocity_10min"]["max_count"]:
        return {
            "rule":    "velocity_10min",
            "details": f"{len(window)} transactions en 10min (max={RULES['velocity_10min']['max_count']})",
            "count":   len(window)
        }
    return None

def check_amount_10min(window, txn):
    """Règle 3 : somme > 2000 en 10 minutes"""
    clean_window(window, 600)
    total = sum(e["amount"] for e in window)
    if total > RULES["amount_10min"]["max_amount"]:
        return {
            "rule":    "amount_10min",
            "details": f"Total {total:.2f} en 10min (max={RULES['amount_10min']['max_amount']})",
            "total":   total
        }
    return None

def check_geo_impossibility(window, txn):
    """Règle 4 : plus d'un pays en 4 heures"""
    clean_window(window, 14400)
    countries = set(e["location"].split(",")[1] for e in window if "," in e["location"])
    current_country = txn.get("location","").split(",")[1] if "," in txn.get("location","") else ""
    if current_country:
        countries.add(current_country)
    if len(countries) > RULES["geo_4h"]["max_countries"]:
        return {
            "rule":    "geo_impossibility",
            "details": f"Pays distincts: {countries}",
            "countries": list(countries)
        }
    return None

def evaluate_rules(card_id, txn):
    """Applique toutes les règles sur la fenêtre de la carte"""
    window  = card_windows[card_id]
    alerts  = []

    alert = check_velocity_60s(window, txn)
    if alert:
        alerts.append(alert)

    alert = check_velocity_10min(window, txn)
    if alert:
        alerts.append(alert)

    alert = check_amount_10min(window, txn)
    if alert:
        alerts.append(alert)

    alert = check_geo_impossibility(window, txn)
    if alert:
        alerts.append(alert)

    return alerts

def build_alert(txn, triggered_rules):
    """Construit le message d'alerte"""
    return {
        "alert_id":       f"ALERT-{txn['transaction_id']}",
        "transaction_id": txn["transaction_id"],
        "card_id":        txn["card_id"],
        "amount":         txn["amount"],
        "location":       txn.get("location",""),
        "merchant_id":    txn.get("merchant_id",""),
        "timestamp":      txn.get("timestamp",""),
        "alert_time":     datetime.utcnow().isoformat(),
        "triggered_rules":triggered_rules,
        "rule_count":     len(triggered_rules),
        "severity":       "HIGH" if len(triggered_rules) >= 2 else "MEDIUM"
    }

def validate_txn(txn):
    """Valide les champs requis"""
    required = ["transaction_id","card_id","amount","timestamp"]
    return all(k in txn for k in required)

def main():
    print("=" * 55)
    print("  SENTINEL STREAM — Stream Processor")
    print(f"  Input  : {TOPIC_INPUT}")
    print(f"  Alerts : {TOPIC_ALERTS}")
    print("=" * 55)

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
            txn = msg.value

            # Validation
            if not validate_txn(txn):
                producer.send(TOPIC_DLQ, key="invalid", value=txn)
                print(f"  [DLQ] Transaction invalide : {txn}")
                continue

            card_id = txn["card_id"]
            txn_count += 1

            # Évalue les règles AVANT d'ajouter à la fenêtre
            triggered = evaluate_rules(card_id, txn)

            # Ajoute la transaction à la fenêtre glissante
            card_windows[card_id].append({
                "ts":       datetime.utcnow(),
                "amount":   float(txn["amount"]),
                "location": txn.get("location","")
            })

            # Si des règles sont déclenchées → alerte
            if triggered:
                alert = build_alert(txn, triggered)
                producer.send(
                    TOPIC_ALERTS,
                    key=card_id,
                    value=alert
                )
                alert_count += 1
                rules_str = " | ".join(r["rule"] for r in triggered)
                print(f"🚨 ALERTE [{alert['severity']}] card={card_id} "
                      f"rules=[{rules_str}] amount={txn['amount']}")
            else:
                if txn_count % 20 == 0:
                    print(f"✅ {txn_count} txn traitées — {alert_count} alertes générées")

    except KeyboardInterrupt:
        print(f"\nArrêt — {txn_count} txn traitées, {alert_count} alertes")
    finally:
        consumer.close()
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
