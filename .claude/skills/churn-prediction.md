# skill: churn-prediction
# Trigger: "churn", "churn prediction", "churn model", "retention", "at-risk customers",
#          "churn rate", "customer lifetime", "early warning", "churn score",
#          "RFM", "recency frequency monetary", "customer health score"

## Purpose
End-to-end churn prediction for Core&Outline's SaaS/fintech/agritech clients.
Covers: feature engineering (RFM + behavioral), XGBoost/LGBM modeling,
SHAP explainability, early warning system, and retention intervention routing.

---

## Feature Engineering

```python
# ml/churn/features.py
"""
Churn feature engineering.
Three feature families: RFM (behavioral), engagement, and contextual.
All features are computed at the customer level for a given reference date.
"""

import pandas as pd
import numpy as np
from datetime import date, timedelta
from dataclasses import dataclass


@dataclass
class ChurnFeatureConfig:
    reference_date: date = None               # compute features "as of" this date
    recency_windows: list = None              # days windows: [7, 14, 30, 60, 90]
    min_tenure_days: int = 14                 # exclude customers with < 14 days
    target_window_days: int = 30              # predict churn in next 30 days

    def __post_init__(self):
        if self.reference_date is None:
            self.reference_date = date.today()
        if self.recency_windows is None:
            self.recency_windows = [7, 14, 30, 60, 90]


def build_rfm_features(
    events: pd.DataFrame,          # user events: customer_id, event_type, event_time, value
    reference_date: date,
    windows: list[int] = (7, 14, 30, 60, 90),
) -> pd.DataFrame:
    """
    Recency, Frequency, Monetary features across multiple time windows.
    RFM is the strongest baseline for churn — compute it first.
    """
    ref_ts = pd.Timestamp(reference_date)
    events["event_time"] = pd.to_datetime(events["event_time"])
    events["days_ago"] = (ref_ts - events["event_time"]).dt.days

    features_list = []
    for customer_id, grp in events.groupby("customer_id"):
        row = {"customer_id": customer_id}

        # Recency: days since last event
        row["recency_days"] = grp["days_ago"].min()
        row["recency_days_log"] = np.log1p(row["recency_days"])

        for w in windows:
            mask = grp["days_ago"] <= w
            # Frequency in window
            row[f"freq_{w}d"] = mask.sum()
            # Monetary in window (e.g. transaction value, usage units)
            row[f"monetary_{w}d"] = grp.loc[mask, "value"].sum() if "value" in grp.columns else 0
            # Trend: frequency ratio last window vs prior window
            if w > 7:
                prev_mask = (grp["days_ago"] > w // 2) & (grp["days_ago"] <= w)
                recent_mask = grp["days_ago"] <= w // 2
                row[f"freq_trend_{w}d"] = (
                    grp[recent_mask].shape[0] / max(grp[prev_mask].shape[0], 1)
                )

        features_list.append(row)

    return pd.DataFrame(features_list)


def build_subscription_features(
    subscriptions: pd.DataFrame,   # customer_id, plan_id, mrr, status, created_at, updated_at
    reference_date: date,
) -> pd.DataFrame:
    """Subscription-level features: plan tier, tenure, MRR, payment issues."""
    ref_ts = pd.Timestamp(reference_date)
    subs = subscriptions[subscriptions["status"].isin(["active", "past_due", "trial"])].copy()

    subs["tenure_days"] = (ref_ts - pd.to_datetime(subs["created_at"])).dt.days
    subs["days_since_update"] = (ref_ts - pd.to_datetime(subs["updated_at"])).dt.days

    plan_tiers = {"starter": 1, "growth": 2, "professional": 3, "enterprise": 4}
    subs["plan_tier_num"] = subs["plan_id"].map(plan_tiers).fillna(0)

    return subs[[
        "customer_id", "tenure_days", "days_since_update",
        "mrr", "plan_tier_num",
        "is_trial", "has_payment_issue",
    ]].rename(columns={
        "mrr": "current_mrr",
    })


def build_support_features(
    tickets: pd.DataFrame,         # customer_id, created_at, status, priority, resolved_at
    reference_date: date,
    window_days: int = 30,
) -> pd.DataFrame:
    """Support ticket signals — strong churn predictor when unresolved."""
    ref_ts = pd.Timestamp(reference_date)
    cutoff = ref_ts - pd.Timedelta(days=window_days)
    recent = tickets[pd.to_datetime(tickets["created_at"]) >= cutoff]

    agg = recent.groupby("customer_id").agg(
        support_tickets_30d=("created_at", "count"),
        unresolved_tickets=("resolved_at", lambda x: x.isna().sum()),
        high_priority_tickets=("priority", lambda x: (x == "high").sum()),
        avg_resolution_days=(
            "resolved_at",
            lambda x: (
                (pd.to_datetime(x) - pd.to_datetime(recent.loc[x.index, "created_at"]))
                .dt.days.mean()
            ),
        ),
    ).reset_index()
    agg["unresolved_ratio"] = agg["unresolved_tickets"] / agg["support_tickets_30d"].clip(lower=1)
    return agg


def build_all_features(
    events: pd.DataFrame,
    subscriptions: pd.DataFrame,
    tickets: pd.DataFrame,
    config: ChurnFeatureConfig,
) -> pd.DataFrame:
    """Merge all feature families into a single customer-level feature matrix."""
    rfm = build_rfm_features(events, config.reference_date, config.recency_windows)
    sub_feats = build_subscription_features(subscriptions, config.reference_date)
    support_feats = build_support_features(tickets, config.reference_date)

    features = rfm.merge(sub_feats, on="customer_id", how="left")
    features = features.merge(support_feats, on="customer_id", how="left")

    # Fill missing (customers with no support tickets, etc.)
    ticket_cols = [c for c in features.columns if "ticket" in c or "resolution" in c]
    features[ticket_cols] = features[ticket_cols].fillna(0)

    # Filter by minimum tenure
    features = features[features["tenure_days"] >= config.min_tenure_days]

    return features
```

---

## Model Training

```python
# ml/churn/model.py

import xgboost as xgb
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import roc_auc_score, average_precision_score, classification_report
from sklearn.calibration import CalibratedClassifierCV
from dataclasses import dataclass
import shap
import joblib


@dataclass
class ChurnModelConfig:
    algorithm: str = "xgboost"       # 'xgboost' | 'lightgbm'
    n_estimators: int = 500
    max_depth: int = 6
    learning_rate: float = 0.05
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    min_child_weight: int = 5
    scale_pos_weight: float = None    # auto-computed if None (handles class imbalance)
    cv_folds: int = 5
    calibrate: bool = True            # probability calibration (Platt scaling)
    threshold: float = 0.3            # classification threshold (lower = catch more churners)
    output_path: str = "models/churn_model.pkl"


class ChurnPredictor:

    def __init__(self, config: ChurnModelConfig):
        self.config = config
        self.model = None
        self.feature_names = None
        self.explainer = None

    def train(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        eval_set: tuple = None,
    ) -> dict:
        self.feature_names = list(X.columns)

        # Auto-compute class weight for imbalanced churn data
        if self.config.scale_pos_weight is None:
            neg, pos = (y == 0).sum(), (y == 1).sum()
            self.config.scale_pos_weight = neg / max(pos, 1)

        # Cross-validation to get honest estimate
        cv = StratifiedKFold(n_splits=self.config.cv_folds, shuffle=True, random_state=42)
        cv_aucs = []

        for fold, (train_idx, val_idx) in enumerate(cv.split(X, y)):
            X_tr, X_val = X.iloc[train_idx], X.iloc[val_idx]
            y_tr, y_val = y.iloc[train_idx], y.iloc[val_idx]

            if self.config.algorithm == "xgboost":
                model = xgb.XGBClassifier(
                    n_estimators=self.config.n_estimators,
                    max_depth=self.config.max_depth,
                    learning_rate=self.config.learning_rate,
                    subsample=self.config.subsample,
                    colsample_bytree=self.config.colsample_bytree,
                    min_child_weight=self.config.min_child_weight,
                    scale_pos_weight=self.config.scale_pos_weight,
                    eval_metric="aucpr",
                    early_stopping_rounds=50,
                    random_state=42,
                )
                model.fit(X_tr, y_tr, eval_set=[(X_val, y_val)], verbose=False)
            else:
                model = lgb.LGBMClassifier(
                    n_estimators=self.config.n_estimators,
                    max_depth=self.config.max_depth,
                    learning_rate=self.config.learning_rate,
                    subsample=self.config.subsample,
                    colsample_bytree=self.config.colsample_bytree,
                    is_unbalance=True,
                    random_state=42,
                )
                model.fit(X_tr, y_tr,
                          eval_set=[(X_val, y_val)],
                          callbacks=[lgb.early_stopping(50), lgb.log_evaluation(0)])

            val_proba = model.predict_proba(X_val)[:, 1]
            auc = roc_auc_score(y_val, val_proba)
            cv_aucs.append(auc)

        # Train final model on full data
        self.model = model  # keep last fold model
        if self.config.calibrate:
            self.model = CalibratedClassifierCV(model, cv=3, method="sigmoid")
            self.model.fit(X, y)

        # SHAP explainer
        base_model = model.calibrated_classifiers_[0].estimator if self.config.calibrate else model
        self.explainer = shap.TreeExplainer(base_model)

        metrics = {
            "cv_auc_mean": float(np.mean(cv_aucs)),
            "cv_auc_std": float(np.std(cv_aucs)),
            "n_features": len(self.feature_names),
        }
        print(f"Churn model trained | CV AUC: {metrics['cv_auc_mean']:.3f} ± {metrics['cv_auc_std']:.3f}")
        return metrics

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        return self.model.predict_proba(X[self.feature_names])[:, 1]

    def predict_risk_tier(self, X: pd.DataFrame) -> pd.Series:
        """Segment customers into risk tiers for intervention routing."""
        proba = self.predict_proba(X)
        return pd.cut(
            proba,
            bins=[0, 0.2, 0.4, 0.7, 1.01],
            labels=["low", "medium", "high", "critical"],
            right=False,
        )

    def explain(self, X: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
        """Return SHAP-based feature importances for a batch of customers."""
        shap_values = self.explainer.shap_values(X[self.feature_names])
        if isinstance(shap_values, list):
            shap_values = shap_values[1]   # positive class for binary classification

        importance_df = pd.DataFrame(
            np.abs(shap_values).mean(axis=0),
            index=self.feature_names,
            columns=["mean_abs_shap"],
        ).sort_values("mean_abs_shap", ascending=False)
        return importance_df.head(top_n)

    def explain_customer(self, customer_row: pd.Series) -> dict:
        """Explain a single customer's churn score in plain terms."""
        X_single = customer_row[self.feature_names].to_frame().T
        shap_vals = self.explainer.shap_values(X_single)
        if isinstance(shap_vals, list):
            shap_vals = shap_vals[1]
        return dict(zip(self.feature_names, shap_vals[0]))

    def save(self) -> None:
        joblib.dump({"model": self.model, "feature_names": self.feature_names}, self.config.output_path)

    @classmethod
    def load(cls, path: str) -> "ChurnPredictor":
        obj = joblib.load(path)
        instance = cls(ChurnModelConfig())
        instance.model = obj["model"]
        instance.feature_names = obj["feature_names"]
        return instance
```

---

## Early Warning System

```python
# ml/churn/early_warning.py
"""
Scheduled churn scoring + alert routing.
Runs daily via Airflow. Segments at-risk customers and routes to CRM actions.
"""

import pandas as pd
from .model import ChurnPredictor
from .features import build_all_features, ChurnFeatureConfig
from datetime import date
import logging

logger = logging.getLogger(__name__)


INTERVENTION_PLAYBOOK = {
    "critical": {
        "action": "escalate_to_csm",
        "message_template": "churn_risk_critical",
        "sla_hours": 4,
    },
    "high": {
        "action": "send_retention_email",
        "message_template": "churn_risk_high",
        "sla_hours": 24,
    },
    "medium": {
        "action": "in_app_nudge",
        "message_template": "engagement_nudge",
        "sla_hours": 72,
    },
    "low": {
        "action": "none",
        "message_template": None,
        "sla_hours": None,
    },
}


def run_daily_churn_scoring(
    events_df: pd.DataFrame,
    subscriptions_df: pd.DataFrame,
    tickets_df: pd.DataFrame,
    model_path: str = "models/churn_model.pkl",
    output_path: str = "data/churn_scores/",
) -> pd.DataFrame:
    """
    Daily churn scoring pipeline.
    Returns scored customer DataFrame with risk tier and recommended action.
    """
    config = ChurnFeatureConfig(reference_date=date.today())
    features = build_all_features(events_df, subscriptions_df, tickets_df, config)

    predictor = ChurnPredictor.load(model_path)
    features["churn_score"] = predictor.predict_proba(features)
    features["risk_tier"] = predictor.predict_risk_tier(features)
    features["recommended_action"] = features["risk_tier"].map(
        {tier: data["action"] for tier, data in INTERVENTION_PLAYBOOK.items()}
    )

    # Log summary
    tier_dist = features["risk_tier"].value_counts()
    logger.info(f"Churn scoring complete: {tier_dist.to_dict()}")

    # Save scored output
    scored_path = f"{output_path}{date.today()}_churn_scores.parquet"
    features.to_parquet(scored_path, index=False)

    return features


def generate_retention_brief(
    customer_id: str,
    features: pd.DataFrame,
    predictor: ChurnPredictor,
    anthropic_client,
) -> str:
    """
    Generate a plain-English CSM brief explaining why a customer is at risk.
    Uses SHAP feature importances fed into Claude.
    """
    customer_row = features[features["customer_id"] == customer_id].iloc[0]
    shap_scores = predictor.explain_customer(customer_row)

    # Top 5 risk signals
    top_signals = sorted(shap_scores.items(), key=lambda x: abs(x[1]), reverse=True)[:5]
    signal_text = "\n".join([
        f"- {feat}: {'increases' if score > 0 else 'decreases'} risk "
        f"(SHAP: {score:+.3f}, value: {customer_row.get(feat, 'N/A')})"
        for feat, score in top_signals
    ])

    from ml.llm.prompts import CHURN_RISK_EXPLANATION_PROMPT
    prompt = CHURN_RISK_EXPLANATION_PROMPT.format(
        churn_score=customer_row["churn_score"],
        customer_profile=customer_row[["tenure_days", "current_mrr", "plan_tier_num"]].to_dict(),
        behavior_signals=f"Recency: {customer_row.get('recency_days', 'N/A')} days ago | "
                         f"Frequency (30d): {customer_row.get('freq_30d', 0)} events | "
                         f"Support tickets (30d): {customer_row.get('support_tickets_30d', 0)}",
        feature_importances=signal_text,
    )

    response = anthropic_client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text
```

---

## Evaluation Report

```python
# ml/churn/evaluate.py

def generate_churn_eval_report(
    y_true: np.ndarray,
    y_pred_proba: np.ndarray,
    threshold: float = 0.3,
) -> dict:
    from sklearn.metrics import (
        roc_auc_score, average_precision_score,
        precision_recall_curve, confusion_matrix,
    )

    y_pred = (y_pred_proba >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()

    return {
        "auc_roc": roc_auc_score(y_true, y_pred_proba),
        "auc_pr": average_precision_score(y_true, y_pred_proba),   # better for imbalanced
        "precision": tp / max(tp + fp, 1),
        "recall": tp / max(tp + fn, 1),             # catch rate — most important
        "false_positive_rate": fp / max(fp + tn, 1),
        "threshold": threshold,
        "n_alerted": int(tp + fp),
        "n_missed": int(fn),                        # churners not caught
        "confusion_matrix": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
    }
```

---

## Usage in Claude Code

```bash
# Build feature matrix
python -m ml.churn.features \
  --events data/events.parquet \
  --subscriptions data/subscriptions.parquet \
  --tickets data/tickets.parquet \
  --output data/churn_features.parquet

# Train churn model
python -m ml.churn.model --features data/churn_features.parquet --algorithm xgboost

# Run daily scoring
python -m ml.churn.early_warning \
  --events data/ --model models/churn_model.pkl --output data/churn_scores/

# Generate CSM brief for at-risk customer
python -m ml.churn.early_warning generate-brief --customer-id cust_abc123

# Evaluate model
python -m ml.churn.evaluate --scores data/churn_scores/2026-03-14_churn_scores.parquet
```
