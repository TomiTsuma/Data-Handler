# skill: ml-system-design
# Trigger: "system design", "design a system", "how would you build", "architecture",
#          "scale", "high-level design", "interview", "ML design", "end-to-end system",
#          "production ML", "ML platform", "real-time", "batch prediction",
#          "feature store", "model registry", "AB test", "online learning"

## Purpose
ML system design patterns for interviews and actual Core&Outline architecture decisions.
Covers: problem framing, architecture choices, scale estimation, trade-off analysis,
and the standard design templates for common ML system types.

---

## System Design Framework (use for every design)

```
1. CLARIFY REQUIREMENTS (2 min)
   - Scale: daily users, QPS, data volume
   - Latency: real-time (<100ms) vs near-real-time (<1s) vs batch (hours)
   - Accuracy vs speed trade-off
   - Freshness: how stale can predictions be?
   - Cold start: how to handle new users/items?

2. HIGH-LEVEL ARCHITECTURE (5 min)
   - Data layer (ingestion, storage, feature engineering)
   - ML layer (training pipeline, model serving)
   - Application layer (API, caching, fallbacks)

3. DATA PIPELINE DESIGN (5 min)
   - Training data: sources, labeling, volume
   - Feature engineering: real-time vs batch features
   - Feature store: online (Redis) vs offline (S3 + DuckDB)

4. MODEL DESIGN (5 min)
   - Algorithm selection with justification
   - Architecture (single model vs ensemble vs cascade)
   - Training: batch vs online, frequency

5. SERVING ARCHITECTURE (5 min)
   - Batch pre-compute vs real-time inference
   - Caching strategy
   - Fallback policies (model fails → rule-based)
   - A/B testing infrastructure

6. EVALUATION & MONITORING (3 min)
   - Offline metrics (AUC, NDCG, RMSE)
   - Online metrics (click-through, conversion, revenue)
   - Data drift detection
   - Model degradation alerts

7. SCALE & OPTIMIZATION (3 min)
   - Bottlenecks and how to address them
   - Cost vs latency trade-offs
```

---

## Design Templates

### Recommendation System (Core&Outline + Interviews)

```
PROBLEM: Recommend relevant analytics insights/reports to business users.

SCALE:
- 10K businesses, 50K users, 1M insight events/day
- Latency: <200ms for real-time recommendations
- Freshness: update models daily, feature store refreshed hourly

ARCHITECTURE:

[OFFLINE PIPELINE]
User events (Airflow, daily)
    → Feature Engineering (behavioral RFM, subscription tier)
    → Feature Store (offline: S3 Parquet + dbt)
    → Model Training (3-stage pipeline, weekly retrain)
    → Model Registry (MLflow, S3 artifacts)

[ONLINE PIPELINE]
User request
    → Feature Store (online: Redis, <10ms lookup)
    → Stage 1: Candidate Generation
        - Collaborative filtering embeddings (pre-computed, FAISS)
        - Content-based (insight category match)
        - Combines via union: ~200 candidates
    → Stage 2: Ranking (XGBoost, 50 features, ~50ms)
    → Stage 3: Diversity reranking (MMR, ~5ms)
    → Response cache (Redis, 5min TTL)
    → API response

COLD START:
- New user (no history): popularity + onboarding survey preferences
- New insight: content-based features only until 10 interactions

MONITORING:
- Online: CTR on recommendations, insight engagement rate
- Offline: NDCG@10, Hit Rate@10 on held-out test set
- Drift: distribution shift in user embedding space (monthly PCA check)
- Alert if CTR drops >15% day-over-day

TRADE-OFFS DISCUSSED:
- Matrix factorization vs deep learning: MF wins on cold start, DL wins on feature richness
- Pre-compute vs real-time: pre-compute embeddings (offline), rank in real-time
- Diversity vs relevance: lambda_mmr=0.7 (tunable per business type)
```

---

### Churn Prediction System

```
PROBLEM: Predict which SaaS customers will churn in the next 30 days.

SCALE:
- 100K customers across 500 businesses
- Batch predictions (not real-time): run nightly
- Latency: 24-hour delay acceptable
- Volume: 10M events/day

ARCHITECTURE:

[DATA LAYER]
Events (Airflow DAG, nightly)
    → Raw events → Parquet on S3 (partitioned by date)
    → dbt transformations → Feature materialization
    → Feature store (offline: S3, updated nightly)

[TRAINING PIPELINE] (weekly)
Feature store (90-day window)
    → Label generation (churned in next 30 days = 1)
    → Feature engineering (RFM + subscription + support)
    → XGBoost training with CV
    → SHAP importance validation
    → MLflow registration
    → Promote if AUC > baseline

[SCORING PIPELINE] (nightly, Airflow)
All active customers
    → Feature retrieval (S3 Parquet)
    → Batch inference (XGBoost, ~50ms/1K customers)
    → Risk tier segmentation (low/medium/high/critical)
    → Write to PostgreSQL (churn_scores table)
    → Trigger CRM actions (Slack alerts, email sequences)

LABEL STRATEGY:
- Positive: customer cancelled within next 30 days
- Negative: customer active 30 days later
- Handle imbalance: scale_pos_weight = neg/pos (~20:1 typical)

MONITORING:
- Precision/Recall at 30-day threshold
- Track false positive rate (don't over-alert CSMs)
- Feature distribution drift (PSI score weekly)
- Alert if AUC drops > 0.03 vs baseline
```

---

### Dynamic Pricing System (Core&Outline)

```
PROBLEM: Set optimal prices for items to maximize margin × conversion.

SCALE:
- 10K SKUs, price updated hourly
- Competitor prices scraped every 15 minutes
- Demand signal: real-time (inventory, browse events)
- Revenue impact: direct → high reliability requirement

ARCHITECTURE:

[DATA LAYER]
Competitor price scraper (15min)
    → Redis (live competitor prices, 20min TTL)
Historical pricing data
    → S3 Parquet → DuckDB for demand elasticity analysis

[MODEL LAYER]
Demand Forecaster (batch, daily retrain)
    → XGBoost regression: features → predicted demand at price P
    → Serves as environment simulator for RL

DQfD Pricing Agent (trained weekly)
    → State: competitor prices + demand signal + inventory + time features
    → Action: price tier (discrete) or price multiplier (continuous SAC)
    → Reward: margin × predicted conversion - penalties
    → Pretrained on 12 months of historical pricing decisions

KVI Detector (batch, weekly)
    → Identifies price-sensitive items (DuckDB KVI query)
    → High-KVI items: tighter pricing bands, more aggressive competitor tracking
    → Low-KVI items: wider bands, less frequent updates

[SERVING LAYER]
Pricing request (item_id, business_id)
    → Feature assembly (Redis lookups: competitor, demand, inventory)
    → DQfD policy inference (<20ms)
    → Business rule guardrails (min/max margins, competitor parity)
    → Price recommendation
    → A/B test framework (new policy vs current rule-based)

SAFETY GUARDRAILS:
- Price floor: cost × 1.05 (never sell below margin)
- Price ceiling: competitor_min × 2.0 (prevent price gouging alerts)
- Volatility dampening: max 15% change per period
- Manual override: CSM can lock any item's price for 24h

MONITORING:
- Revenue per unit vs rule-based baseline
- Conversion rate by price tier
- Competitor price gap tracking
- Policy divergence alerts (when RL diverges > 20% from rules)
```

---

### Real-Time Fraud Detection (Fintech Context)

```
PROBLEM: Detect fraudulent transactions in real-time (<100ms).

SCALE:
- 10K transactions/minute peak
- P99 latency requirement: <100ms
- False positive tolerance: <0.1% (don't block legit txns)
- False negative tolerance: <2% (miss rate)

ARCHITECTURE:

[REAL-TIME PIPELINE]
Transaction event (Kafka)
    → Feature assembly (<10ms, Redis lookups)
        - User features: velocity (1min, 5min, 1hr windows)
        - Device features: new device, location change
        - Merchant features: category, historical fraud rate
    → Model inference (<30ms)
        - Tier 1: LightGBM (20ms) → if score < 0.3, approve immediately
        - Tier 2: Neural network (50ms) → for scores 0.3-0.7
    → Rules engine (velocity checks, blocklist)
    → Decision: approve / flag / block

[OFFLINE PIPELINE]
Historical transactions (Spark, daily)
    → Positive labels: confirmed fraud (from dispute resolution)
    → Feature engineering (session, device, behavioral)
    → Weekly model retraining
    → Shadow mode deployment before promotion

FEATURE VELOCITY (real-time features in Redis):
- tx_count_1min, tx_count_5min, tx_count_1hr
- amount_sum_1hr, amount_sum_24hr
- unique_merchants_24hr
- location_change_flag, device_change_flag

DECISION THRESHOLD TUNING:
- Business sets tolerance: "catch 98% of fraud, <0.1% false positive"
- Adjust threshold on ROC curve to meet both constraints
- Separate thresholds by transaction type (high-value vs micro-tx)
```

---

## Interview Questions & Model Answers

### "Design a feed ranking system"
```
Key insight: it's a recommendation system where freshness matters.

Algorithm: Two-tower neural network (user + content towers → dot product score)
+ chronological freshness decay: final_score = model_score × e^(-λ × hours_old)

Feature families:
- User: interaction history, follows, session context
- Content: engagement velocity (likes/min), author authority, content type
- User-content affinity: historical interaction with similar content

Serving: pre-compute content embeddings, real-time user tower at inference
Trade-off: staleness vs compute cost (pre-compute user embeddings hourly)
```

### "How do you handle model drift?"
```
Three types — different detection, different response:

1. Data drift (input distribution shifts)
   - Detect: Population Stability Index (PSI) on feature distributions
   - Response: retrain on recent data, investigate source

2. Concept drift (relationship between features and labels changes)
   - Detect: monitor online metrics (CTR, conversion) vs baseline
   - Response: collect fresh labels, retrain, shadow deploy

3. Feedback loops (model predictions change future data)
   - Example: churn model flags customers → CSM saves them → model sees no churn → underestimates risk
   - Detect: compare predicted churn rate vs actual churn rate over time
   - Response: counterfactual logging, uplift modeling
```

### "How do you evaluate a recommendation system?"
```
Offline (fast, cheap, but not ground truth):
- NDCG@K: quality of ranking
- Hit Rate@K: fraction of users with ≥1 relevant item in top-K
- Intra-list Diversity: average pairwise distance in recommendations
- Coverage: fraction of item catalog surfaced

Online (ground truth, expensive, slow):
- CTR: click-through rate on recommendations
- Conversion rate: downstream business action taken
- Revenue lift: revenue from recommended items vs control
- Session length: do recommendations keep users engaged?

Always A/B test before relying on offline metrics.
Offline NDCG improvements don't always translate to online wins.
```

---

## Estimation Framework (Scale Questions)

```python
# Quick napkin math for scale estimation

DAILY_USERS = 50_000
SESSIONS_PER_USER = 3
EVENTS_PER_SESSION = 20
EVENTS_PER_DAY = DAILY_USERS * SESSIONS_PER_USER * EVENTS_PER_SESSION  # 3M/day

# QPS estimation
PEAK_QPS = DAILY_USERS * SESSIONS_PER_USER / (8 * 3600)  # 8 active hours
PEAK_QPS *= 3  # 3× average for peak traffic ≈ 52 QPS

# Storage estimation
EVENTS_SIZE_BYTES = EVENTS_PER_DAY * 200  # 200 bytes per event = 600MB/day
ANNUAL_STORAGE_GB = EVENTS_SIZE_BYTES * 365 / 1e9  # ~220 GB/year

# Model inference
INFERENCE_LATENCY_MS = 50  # XGBoost
MAX_QPS_SINGLE_INSTANCE = 1000 / 50  # = 20 QPS per instance
INSTANCES_NEEDED = PEAK_QPS / (MAX_QPS_SINGLE_INSTANCE * 0.7)  # 70% util = ~3.7 → 4 instances
```
