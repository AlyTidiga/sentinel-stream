"""
Sentinel Stream — Entraînement XGBoost + MLflow tracking
"""
import pandas as pd
import numpy as np
import mlflow
import mlflow.xgboost
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    classification_report, roc_auc_score,
    f1_score, precision_score, recall_score,
    confusion_matrix
)
from sklearn.utils.class_weight import compute_class_weight

MLFLOW_URI   = "http://localhost:5000"
EXPERIMENT   = "sentinel-fraud-detection"
DATASET_PATH = "ml/transactions_dataset.csv"

FEATURES = [
    "amount", "hour", "txn_count_60s",
    "txn_count_10min", "amount_sum_10min",
    "distinct_countries_4h", "is_weekend", "is_foreign"
]

def main():
    print("=" * 55)
    print("  SENTINEL STREAM — Training XGBoost")
    print("=" * 55)

    # ── Chargement des données ──
    df = pd.read_csv(DATASET_PATH)
    print(f"\nDataset : {len(df)} transactions — {df['label'].sum()} fraudes")

    X = df[FEATURES]
    y = df["label"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    print(f"Train : {len(X_train)} | Test : {len(X_test)}")

    # ── MLflow setup ──
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)

    # ── Paramètres du modèle ──
    classes = np.unique(y_train)
    weights = compute_class_weight("balanced", classes=classes, y=y_train)
    scale_pos_weight = weights[1] / weights[0]
    params = {
        "n_estimators":     200,
        "max_depth":        6,
        "learning_rate":    0.1,
        "scale_pos_weight": float(scale_pos_weight),
        "subsample":        0.8,
        "colsample_bytree": 0.8,
        "random_state":     42,
        "eval_metric":      "logloss",
    }

    print(f"\nEntraînement XGBoost...")
    print(f"scale_pos_weight = {params['scale_pos_weight']:.2f}")

    with mlflow.start_run(run_name="xgboost-v1"):

        # ── Entraînement ──
        model = XGBClassifier(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False
        )

        # ── Métriques ──
        y_pred      = model.predict(X_test)
        y_pred_prob = model.predict_proba(X_test)[:, 1]

        auc       = roc_auc_score(y_test, y_pred_prob)
        f1        = f1_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred)
        recall    = recall_score(y_test, y_pred)
        cm        = confusion_matrix(y_test, y_pred)

        print(f"\n{'='*40}")
        print(f"  AUC-ROC   : {auc:.4f}")
        print(f"  F1-Score  : {f1:.4f}")
        print(f"  Precision : {precision:.4f}")
        print(f"  Recall    : {recall:.4f}")
        print(f"{'='*40}")
        print(f"\nMatrice de confusion :")
        print(f"  TN={cm[0][0]}  FP={cm[0][1]}")
        print(f"  FN={cm[1][0]}  TP={cm[1][1]}")
        print(f"\n{classification_report(y_test, y_pred, target_names=['Legit','Fraud'])}")

        # ── Log MLflow ──
        mlflow.log_params(params)
        mlflow.log_metrics({
            "auc_roc":   auc,
            "f1_score":  f1,
            "precision": precision,
            "recall":    recall,
            "tn": int(cm[0][0]),
            "fp": int(cm[0][1]),
            "fn": int(cm[1][0]),
            "tp": int(cm[1][1]),
        })

        import os, pickle
        os.makedirs("ml/models", exist_ok=True)
        with open("ml/models/fraud_model.pkl", "wb") as f:
            pickle.dump(model, f)
        print("Modele sauvegarde : ml/models/fraud_model.pkl")
        run_id = mlflow.active_run().info.run_id
        print(f"\nModèle enregistré dans MLflow")
        print(f"Run ID : {run_id}")
        print(f"\nOuvre http://localhost:5000 pour voir les résultats")

if __name__ == "__main__":
    main()
