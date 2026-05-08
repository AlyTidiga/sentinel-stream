import pickle
import time
import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter, Histogram, Gauge

with open("models/fraud_model.pkl", "rb") as f:
    model = pickle.load(f)

app = FastAPI(title="Sentinel Stream - Fraud Scoring API")

FRAUD_BLOCKED     = Counter("sentinel_fraud_blocked_total",  "Transactions bloquees")
FRAUD_REVIEW      = Counter("sentinel_fraud_review_total",   "Transactions en revue")
FRAUD_APPROVED    = Counter("sentinel_fraud_approved_total", "Transactions approuvees")
SCORING_LATENCY   = Histogram("sentinel_scoring_latency_ms", "Latence scoring ML",
                               buckets=[1,2,5,10,20,50,100,200])
FRAUD_SCORE_GAUGE = Gauge("sentinel_last_fraud_score", "Dernier score fraude")

Instrumentator().instrument(app).expose(app)

class Transaction(BaseModel):
    amount: float
    hour: int
    txn_count_60s: int
    txn_count_10min: int
    amount_sum_10min: float
    distinct_countries_4h: int
    is_weekend: int
    is_foreign: int

@app.get("/health")
def health():
    return {"status": "ok", "model": "xgboost-fraud-v1"}

@app.post("/score")
def score(txn: Transaction):
    start = time.time()
    features = np.array([[
        txn.amount, txn.hour, txn.txn_count_60s,
        txn.txn_count_10min, txn.amount_sum_10min,
        txn.distinct_countries_4h, txn.is_weekend, txn.is_foreign
    ]])
    fraud_score = float(model.predict_proba(features)[0][1])
    latency_ms  = round((time.time() - start) * 1000, 2)
    if fraud_score >= 0.85:
        decision = "BLOCK"
        FRAUD_BLOCKED.inc()
    elif fraud_score >= 0.60:
        decision = "REVIEW"
        FRAUD_REVIEW.inc()
    else:
        decision = "APPROVE"
        FRAUD_APPROVED.inc()
    SCORING_LATENCY.observe(latency_ms)
    FRAUD_SCORE_GAUGE.set(fraud_score)
    return {"fraud_score": round(fraud_score, 4), "decision": decision, "latency_ms": latency_ms}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
