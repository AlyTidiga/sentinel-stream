"""
Dataset réaliste — overlap intentionnel entre fraude et légit
"""
import pandas as pd
import numpy as np
import random

random.seed(42)
np.random.seed(42)

N_LEGIT = 10000
N_FRAUD = 500

def gen_legit():
    rows = []
    for _ in range(N_LEGIT):
        rows.append({
            "amount":                round(np.random.exponential(150) + np.random.normal(0, 30), 2),
            "hour":                  np.random.choice(range(24)),
            "txn_count_60s":         np.random.choice([1,2,3,4], p=[0.60,0.25,0.10,0.05]),
            "txn_count_10min":       np.random.randint(1, 10),
            "amount_sum_10min":      round(np.random.uniform(10, 1500), 2),
            "distinct_countries_4h": np.random.choice([1,2], p=[0.92,0.08]),
            "is_weekend":            np.random.choice([0,1], p=[0.70,0.30]),
            "is_foreign":            np.random.choice([0,1], p=[0.85,0.15]),
            "label":                 0
        })
    return rows

def gen_fraud():
    rows = []
    for _ in range(N_FRAUD):
        fraud_type = random.choice(["velocity","geo","high_amount","mixed"])
        if fraud_type == "velocity":
            rows.append({
                "amount":                round(np.random.uniform(5, 200) + np.random.normal(0, 20), 2),
                "hour":                  np.random.choice(range(24)),
                "txn_count_60s":         np.random.choice([3,4,5,6,7], p=[0.20,0.30,0.25,0.15,0.10]),
                "txn_count_10min":       np.random.randint(5, 20),
                "amount_sum_10min":      round(np.random.uniform(50, 800), 2),
                "distinct_countries_4h": np.random.choice([1,2], p=[0.80,0.20]),
                "is_weekend":            np.random.choice([0,1]),
                "is_foreign":            np.random.choice([0,1], p=[0.70,0.30]),
                "label":                 1
            })
        elif fraud_type == "geo":
            rows.append({
                "amount":                round(np.random.uniform(100, 1000) + np.random.normal(0, 50), 2),
                "hour":                  np.random.choice(range(24)),
                "txn_count_60s":         np.random.choice([1,2,3], p=[0.50,0.30,0.20]),
                "txn_count_10min":       np.random.randint(1, 8),
                "amount_sum_10min":      round(np.random.uniform(100, 2000), 2),
                "distinct_countries_4h": np.random.choice([2,3], p=[0.70,0.30]),
                "is_weekend":            np.random.choice([0,1]),
                "is_foreign":            1,
                "label":                 1
            })
        elif fraud_type == "high_amount":
            rows.append({
                "amount":                round(np.random.uniform(1500, 9999) + np.random.normal(0, 200), 2),
                "hour":                  np.random.choice(range(24)),
                "txn_count_60s":         np.random.choice([1,2], p=[0.70,0.30]),
                "txn_count_10min":       np.random.randint(1, 5),
                "amount_sum_10min":      round(np.random.uniform(1500, 9999), 2),
                "distinct_countries_4h": np.random.choice([1,2], p=[0.60,0.40]),
                "is_weekend":            np.random.choice([0,1]),
                "is_foreign":            np.random.choice([0,1], p=[0.50,0.50]),
                "label":                 1
            })
        else:
            # Mixed — pattern ambigu, difficile à détecter
            rows.append({
                "amount":                round(np.random.uniform(50, 500) + np.random.normal(0, 40), 2),
                "hour":                  np.random.choice(range(24)),
                "txn_count_60s":         np.random.choice([2,3,4], p=[0.40,0.35,0.25]),
                "txn_count_10min":       np.random.randint(3, 12),
                "amount_sum_10min":      round(np.random.uniform(100, 1200), 2),
                "distinct_countries_4h": np.random.choice([1,2], p=[0.65,0.35]),
                "is_weekend":            np.random.choice([0,1]),
                "is_foreign":            np.random.choice([0,1], p=[0.60,0.40]),
                "label":                 1
            })
    return rows

rows = gen_legit() + gen_fraud()
random.shuffle(rows)
df = pd.DataFrame(rows)
df["amount"] = df["amount"].clip(lower=1.0)
df.to_csv("ml/transactions_dataset.csv", index=False)
print(f"Dataset : {len(df)} transactions — {df['label'].sum()} fraudes ({df['label'].mean()*100:.1f}%)")
