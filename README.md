# Sentinel Stream
Real-time Fraud Detection Pipeline

## Stack
- Apache Kafka
- Stream Processor Python
- XGBoost + MLflow (AUC 0.98)
- FastAPI scoring < 10ms
- Prometheus + Grafana
- Docker Compose
- Pytest 17/17 tests

## Demarrage
docker compose --profile monitoring up -d

## Interfaces
- Kafka UI : http://localhost:8080
- MLflow   : http://localhost:5000
- Grafana  : http://localhost:3000
- ML API   : http://localhost:8000/docs

## Resultats
- Latence scoring : < 10ms
- AUC-ROC : 0.9808
- Tests : 17/17
- Decisions : BLOCK / REVIEW / APPROVE
