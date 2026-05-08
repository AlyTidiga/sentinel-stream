import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

KAFKA_BROKER = "localhost:9092"
TOPIC        = "txn-raw"
TXN_PER_SEC  = 5

LEGIT_CARDS = [f"CARD-{i:04d}" for i in range(1, 51)]
FRAUD_CARDS = [f"FRAUD-{i:04d}" for i in range(1, 6)]

MERCHANTS = [
    "CARREFOUR_DAKAR", "TOTAL_ENERGIE", "ORANGE_MONEY",
    "AMAZON_EU", "RESTAURANT_TERANGA", "HOTEL_RADISSON",
    "ATM_ECOBANK", "PHARMACIE_PLUS", "AIR_SENEGAL",
    "NETFLIX", "UBER", "ZARA_ONLINE"
]

LOCATIONS = [
    "Dakar,SN", "Dakar,SN", "Dakar,SN",
    "Paris,FR", "Abidjan,CI", "Accra,GH",
    "Dubai,AE", "London,UK", "New York,US"
]

def generate_legit_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id":        random.choice(LEGIT_CARDS),
        "merchant_id":    random.choice(MERCHANTS),
        "amount":         round(random.uniform(5.0, 500.0), 2),
        "currency":       "XOF",
        "location":       random.choices(LOCATIONS, weights=[40,40,10,3,3,1,1,1,1])[0],
        "timestamp":      datetime.utcnow().isoformat(),
        "is_fraud_sim":   False
    }

def generate_fraud_transaction(fraud_type):
    card = random.choice(FRAUD_CARDS)
    if fraud_type == "velocity":
        return {
            "transaction_id": str(uuid.uuid4()),
            "card_id":        card,
            "merchant_id":    random.choice(MERCHANTS),
            "amount":         round(random.uniform(10.0, 50.0), 2),
            "currency":       "XOF",
            "location":       "Dakar,SN",
            "timestamp":      datetime.utcnow().isoformat(),
            "is_fraud_sim":   True,
            "fraud_type":     "velocity"
        }
    elif fraud_type == "geo":
        return {
            "transaction_id": str(uuid.uuid4()),
            "card_id":        card,
            "merchant_id":    "ATM_INTERNATIONAL",
            "amount":         round(random.uniform(200.0, 800.0), 2),
            "currency":       "EUR",
            "location":       random.choice(["Dubai,AE", "London,UK", "New York,US"]),
            "timestamp":      datetime.utcnow().isoformat(),
            "is_fraud_sim":   True,
            "fraud_type":     "geo_impossible"
        }
    else:
        return {
            "transaction_id": str(uuid.uuid4()),
            "card_id":        card,
            "merchant_id":    "LUXURY_STORE",
            "amount":         round(random.uniform(2000.0, 9999.0), 2),
            "currency":       "EUR",
            "location":       "Paris,FR",
            "timestamp":      datetime.utcnow().isoformat(),
            "is_fraud_sim":   True,
            "fraud_type":     "high_amount"
        }

def on_success(metadata):
    print(f"  ok topic={metadata.topic} partition={metadata.partition} offset={metadata.offset}")

def on_error(e):
    print(f"  erreur : {e}")

def main():
    print("=" * 55)
    print("  SENTINEL STREAM - Transaction Producer")
    print(f"  Broker : {KAFKA_BROKER}")
    print(f"  Topic  : {TOPIC}")
    print("=" * 55)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=10,
    )

    count = 0
    fraud_burst_counter = 0
    print("Demarrage du flux... (Ctrl+C pour arreter)")

    try:
        while True:
            count += 1
            fraud_burst_counter += 1

            if fraud_burst_counter >= 30:
                print("FRAUD BURST - 5 txn rapides sur meme carte")
                fraud_card = random.choice(FRAUD_CARDS)
                for _ in range(5):
                    txn = generate_fraud_transaction("velocity")
                    txn["card_id"] = fraud_card
                    producer.send(TOPIC, key=txn["card_id"], value=txn).add_callback(on_success).add_errback(on_error)
                    time.sleep(0.1)
                fraud_burst_counter = 0
                continue

            if count % 15 == 0:
                fraud_type = random.choice(["geo", "high_amt"])
                txn = generate_fraud_transaction(fraud_type)
                print(f"FRAUD {fraud_type} card={txn['card_id']} amount={txn['amount']} loc={txn['location']}")
            else:
                txn = generate_legit_transaction()
                print(f"LEGIT card={txn['card_id']} amount={txn['amount']} loc={txn['location']}")

            producer.send(TOPIC, key=txn["card_id"], value=txn).add_callback(on_success).add_errback(on_error)
            time.sleep(1.0 / TXN_PER_SEC)

    except KeyboardInterrupt:
        print(f"Arret - {count} transactions envoyees")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
