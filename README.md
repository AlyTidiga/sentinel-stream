# Sentinel Stream — Detection de Fraude Bancaire en Temps Reel

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5-black)
![XGBoost](https://img.shields.io/badge/XGBoost-AUC%200.98-green)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Tests](https://img.shields.io/badge/Tests-17%2F17-brightgreen)

## Table des matieres
1. [Contexte et probleme](#contexte)
2. [Architecture](#architecture)
3. [Stack technique](#stack)
4. [Logique de detection](#detection)
5. [Installation](#installation)
6. [Lancer le projet](#lancement)
7. [Interfaces disponibles](#interfaces)
8. [Tests](#tests)
9. [Structure du projet](#structure)
10. [Resultats](#resultats)

---

## 1. Contexte et probleme

Les systemes de detection de fraude traditionnels fonctionnent en mode **batch** :
ils analysent les transactions une fois par jour ou par heure.
Resultat : la fraude est detectee apres que la transaction a deja ete validee et debitee.

**Sentinel Stream** resout ce probleme en interceptant chaque transaction
en temps reel, avant sa validation, en moins de **50 millisecondes**.

| Approche | Delai de detection | Consequence |
|---|---|---|
| Batch (Airflow, cron) | J+1 ou H+1 | Transaction deja debitee, chargeback |
| Sentinel Stream | moins de 50ms | Blocage avant settlement |

---

## 2. Architecture

Le pipeline suit ce flux de bout en bout :

```
[Simulateur Python]
        |
        v
[Apache Kafka] topic: txn-raw
        |
        v
[Stream Processor] -- calcul features --> [Redis Feature Store]
   regles velocite                               |
        |                                        v
        |                              [FastAPI ML API]
        |                              XGBoost scoring
        |                                        |
        +-------- Decision hybride --------------+
                  Rules + ML Score
                        |
           +------------+------------+
           |                         |
    [fraud-alerts]              [APPROVE]
    topic Kafka
           |
           v
    [Prometheus + Grafana]
    monitoring temps reel
```

### Composants detailles

**Producteur Python** : simule un core banking system qui emet des transactions
JSON en continu vers Kafka. Chaque transaction contient :
transaction_id, card_id, merchant_id, amount, currency, location, timestamp.
Le producteur genere des transactions legitimes et des scenarios de fraude
(velocite, geo-impossibilite, montant eleve).

**Apache Kafka** : message broker distribue avec 3 topics :
- txn-raw : toutes les transactions entrantes (6 partitions)
- fraud-alerts : alertes de fraude detectees (3 partitions)
- txn-dead-letter : transactions malformees pour audit

**Stream Processor** : consomme txn-raw et applique en temps reel :
- Des fenetres glissantes pour calculer les features de velocite
- 4 regles deterministes (velocity_60s, velocity_10min, amount_10min, geo_impossibility)
- Un appel a l'API ML pour le scoring XGBoost
- Une decision hybride Rules + ML

**Redis Feature Store** : stocke les features precalculees par carte
pour un acces en moins de 1ms.

**FastAPI ML API** : expose le modele XGBoost via un endpoint REST.
Retourne un score entre 0 et 1 en moins de 10ms.
Expose aussi les metriques Prometheus.

**MLflow** : tracking des experiences d'entrainement.
Logue les hyperparametres, metriques (AUC, F1, precision, recall)
et les artefacts du modele pour chaque run.

**Prometheus + Grafana** : monitoring complet du pipeline.
4 dashboards : transactions bloquees, en revue, approuvees, latence scoring.

---

## 3. Stack technique

| Couche | Technologie | Version | Role |
|---|---|---|---|
| Ingestion | Apache Kafka | 7.5 | Message broker, topics types |
| Streaming | Python Stream Processor | 3.12 | Fenetres glissantes, regles |
| Feature Store | Redis | 7.2 | Cache features moins de 1ms |
| ML Training | XGBoost + MLflow | 2.0 | Entrainement + tracking |
| ML Serving | FastAPI + Docker | 0.100 | Scoring REST moins de 10ms |
| Monitoring | Prometheus + Grafana | 2.49 / 10.3 | Metriques temps reel |
| Tests | Pytest | 9.0 | 17 tests unitaires |
| Orchestration | Docker Compose | 2.20 | Stack complete one-command |

---

## 4. Logique de detection

### 4.1 Regles de velocite (deterministes)

Ces regles sont evaluees a chaque transaction via des fenetres glissantes :

| Regle | Fenetre | Seuil | Explication |
|---|---|---|---|
| velocity_60s | 60 secondes | plus de 3 transactions | Utilisation intense et rapide |
| velocity_10min | 10 minutes | plus de 8 transactions | Detection progressive |
| amount_10min | 10 minutes | plus de 2000 euros cumules | Plafond comportemental |
| geo_impossibility | 4 heures | plus de 1 pays distinct | Impossible physiquement |

### 4.2 Modele ML (XGBoost)

Le modele est entraine sur un dataset de 10500 transactions (500 fraudes, 4.8%).
Features utilisees : amount, hour, txn_count_60s, txn_count_10min,
amount_sum_10min, distinct_countries_4h, is_weekend, is_foreign.

Resultats : AUC-ROC 0.9808, F1-Score 0.7708, Precision 0.8043, Recall 0.7400.

### 4.3 Moteur de decision hybride

| Condition | Score ML | Action | Severite |
|---|---|---|---|
| Regle seule | moins de 0.60 | Alerte equipe | MEDIUM |
| Score ML seul | 0.60 a 0.85 | Revue humaine | MEDIUM |
| Regles + Score | plus de 0.60 | Blocage immediat | HIGH |
| Score critique | plus de 0.85 | Blocage + alerte ops | HIGH |

---

## 5. Installation

### Prerequis

- Windows avec WSL2 (Ubuntu) ou Linux natif
- Docker Desktop 4.x avec integration WSL2 activee
- Python 3.11 ou 3.12
- RAM minimum 6 Go allouee a WSL2
- 10 Go d'espace disque

### Configuration WSL2 (Windows uniquement)

Creez ou editez le fichier C:\Users\VotreNom\.wslconfig :

```
[wsl2]
memory=6GB
processors=4
swap=4GB
```

Puis redemarrez WSL2 depuis PowerShell :

```
wsl --shutdown
```

### Cloner le projet

```
git clone https://github.com/AlyTidiga/sentinel-stream.git
cd sentinel-stream
```

### Creer l'environnement Python

```
python3 -m venv venv
source venv/bin/activate
pip install kafka-python faker scikit-learn xgboost mlflow fastapi uvicorn requests pytest pytest-cov prometheus-fastapi-instrumentator
```

### Construire l'image Docker de l'API ML

```
docker build -t sentinel-ml-api ml/
```

---

## 6. Lancer le projet

### Etape 1 : Demarrer la stack Docker

```
docker compose --profile monitoring up -d
```

Cette commande lance : Zookeeper, Kafka, Schema Registry, Kafka UI,
Flink JobManager, Flink TaskManager, Redis, MLflow, ML API,
Prometheus, Grafana.

Attendez environ 2 minutes que tous les conteneurs soient healthy.

### Etape 2 : Generer le dataset et entrainer le modele

```
python3 ml/generate_dataset.py
python3 ml/train.py
```

Le modele est sauvegarde dans ml/models/fraud_model.pkl
et logue dans MLflow sur http://localhost:5000.

### Etape 3 : Lancer le producteur de transactions

Ouvrez un terminal et lancez :

```
source venv/bin/activate
python3 producers/transaction_producer.py
```

Le producteur emet 5 transactions par seconde.
Toutes les 30 transactions, un burst de fraude est simule
(5 transactions rapides sur la meme carte).

### Etape 4 : Lancer le stream processor

Ouvrez un second terminal et lancez :

```
source venv/bin/activate
python3 flink_jobs/stream_processor_v2.py
```

Le processor consomme les transactions depuis Kafka,
applique les regles de velocite, appelle l'API ML,
et publie les alertes dans le topic fraud-alerts.

Vous verrez s'afficher en temps reel :

```
ALERTE [HIGH] card=FRAUD-0002 score=0.97 decision=BLOCK rules=[velocity_60s, geo_impossibility] latency=5ms
ALERTE [MEDIUM] card=CARD-0014 score=0.03 decision=APPROVE rules=[velocity_60s] latency=3ms
```

---

## 7. Interfaces disponibles

| Interface | URL | Identifiants | Usage |
|---|---|---|---|
| Kafka UI | http://localhost:8080 | aucun | Voir les topics et messages |
| Flink UI | http://localhost:8082 | aucun | Monitorer les jobs |
| MLflow | http://localhost:5000 | aucun | Experiments et modeles |
| Grafana | http://localhost:3000 | admin / sentinel2025 | Dashboards monitoring |
| Prometheus | http://localhost:9090 | aucun | Metriques brutes |
| ML API docs | http://localhost:8000/docs | aucun | Tester le scoring |
| ML API metrics | http://localhost:8000/metrics | aucun | Metriques Prometheus |

### Tester l'API manuellement

Transaction frauduleuse (doit retourner BLOCK) :

```
curl -X POST http://localhost:8000/score
  -H Content-Type: application/json
  -d {amount: 5000, hour: 3, txn_count_60s: 8, txn_count_10min: 15, amount_sum_10min: 8000, distinct_countries_4h: 3, is_weekend: 1, is_foreign: 1}
```

Transaction legitime (doit retourner APPROVE) :

```
curl -X POST http://localhost:8000/score
  -H Content-Type: application/json
  -d {amount: 45, hour: 14, txn_count_60s: 1, txn_count_10min: 2, amount_sum_10min: 90, distinct_countries_4h: 1, is_weekend: 0, is_foreign: 0}
```

---

## 8. Tests

Le projet contient 17 tests unitaires couvrant :
- Les regles de velocite (9 tests)
- La construction des alertes (4 tests)
- L'API ML scoring (4 tests : health, fraude, legitime, latence)

Pour lancer les tests :

```
pytest tests/test_sentinel.py -v
```

Pour lancer avec le rapport de couverture :

```
pytest tests/test_sentinel.py -v --cov=flink_jobs --cov-report=term-missing
```

Resultat attendu : 17 passed en moins de 3 secondes.

---

## 9. Structure du projet

```
sentinel-stream/
├── docker-compose.yml              Stack Docker complete
├── .gitignore
├── README.md
├── producers/
│   └── transaction_producer.py     Simulateur CBS vers Kafka
├── flink_jobs/
│   ├── stream_processor.py         Processor v1 regles seules
│   └── stream_processor_v2.py      Processor v2 Rules + ML
├── ml/
│   ├── Dockerfile                  Image Docker API scoring
│   ├── generate_dataset.py         Generation dataset realiste
│   ├── train.py                    Entrainement XGBoost + MLflow
│   ├── serve.py                    FastAPI + metriques Prometheus
│   └── models/                     Modele entraine (gitignore)
├── monitoring/
│   └── prometheus/
│       └── prometheus.yml          Config scraping Prometheus
└── tests/
    └── test_sentinel.py            17 tests unitaires
```

---

## 10. Resultats

| Metrique | Valeur |
|---|---|
| Latence de scoring | moins de 10ms (p99) |
| AUC-ROC du modele | 0.9808 |
| F1-Score | 0.7708 |
| Precision | 0.8043 |
| Recall | 0.7400 |
| Tests unitaires | 17 sur 17 |
| Transactions bloquees | en temps reel |
| Decisions | BLOCK / REVIEW / APPROVE |

---

## Auteur

AlyTidiga — Data Engineer
Projet portfolio — Mai 2025
GitHub : https://github.com/AlyTidiga/sentinel-stream

Stack : Kafka · Python · XGBoost · MLflow · FastAPI · Docker · Prometheus · Grafana

