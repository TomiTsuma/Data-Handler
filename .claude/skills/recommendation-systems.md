# skill: recommendation-systems
# Trigger: "recommendation", "recommender", "candidate generation", "learn-to-rank",
#          "collaborative filtering", "content-based", "diversity", "reranking",
#          "similar users", "similar items", "FAISS", "ANN", "XGBoost ranking",
#          "LambdaMART", "MMR", "book recommendation", "product recommendation",
#          "skincare", "personalization"

## Purpose
Three-stage recommendation system matching Google's production architecture:
candidate generation → learn-to-rank scoring → diversity reranking.
Covers Tomi's active projects: book recommender, skincare product recommender,
and Core&Outline's analytics-driven recommendation modules.

## Architecture Overview
```
User/Item Data
     │
     ▼
[Stage 1] Candidate Generation
   ANN retrieval (FAISS) + collaborative filtering
   → top-K candidates (e.g. K=200)
     │
     ▼
[Stage 2] Learn-to-Rank Scoring
   XGBoost LambdaMART on rich features
   → ranked list (score per candidate)
     │
     ▼
[Stage 3] Diversity Reranking
   MMR or DPP — reduce redundancy
   → final top-N recommendations
```

---

## Stage 1: Candidate Generation

```python
# ml/recommendations/stage1_candidate_gen.py

from __future__ import annotations
import numpy as np
import faiss
import pandas as pd
from dataclasses import dataclass
from typing import Optional
from sklearn.decomposition import TruncatedSVD
from sklearn.preprocessing import normalize
import scipy.sparse as sp


@dataclass
class CandidateGenConfig:
    n_factors: int = 128          # latent dimension for matrix factorization
    n_candidates: int = 200       # K candidates to retrieve per user
    similarity: str = "cosine"   # 'cosine' | 'dot' | 'euclidean'
    index_type: str = "flat"     # 'flat' (exact) | 'ivf' (approximate, faster)
    n_lists: int = 100            # IVF: number of Voronoi cells
    n_probe: int = 10             # IVF: cells to probe at query time


class CollaborativeFilteringRetriever:
    """
    Matrix factorization (SVD) + FAISS ANN retrieval.
    Builds user and item embeddings from interaction matrix.
    """

    def __init__(self, config: CandidateGenConfig):
        self.config = config
        self.svd = TruncatedSVD(n_components=config.n_factors, random_state=42)
        self.item_index = None     # FAISS index over item embeddings
        self.user_embeddings = None
        self.item_embeddings = None
        self.item_ids = None

    def fit(
        self,
        ratings: pd.DataFrame,    # columns: user_id, item_id, rating
        users: Optional[pd.DataFrame] = None,
        items: Optional[pd.DataFrame] = None,
    ) -> None:
        """Build interaction matrix and factor it."""
        self.user_enc = {uid: i for i, uid in enumerate(ratings["user_id"].unique())}
        self.item_enc = {iid: i for i, iid in enumerate(ratings["item_id"].unique())}
        self.item_ids = list(self.item_enc.keys())

        row = [self.user_enc[u] for u in ratings["user_id"]]
        col = [self.item_enc[i] for i in ratings["item_id"]]
        data = ratings["rating"].values

        n_users = len(self.user_enc)
        n_items = len(self.item_enc)
        interaction_matrix = sp.csr_matrix((data, (row, col)), shape=(n_users, n_items))

        # SVD factorization: interaction_matrix ≈ U × Σ × Vᵀ
        self.user_embeddings = self.svd.fit_transform(interaction_matrix)      # [n_users, k]
        self.item_embeddings = self.svd.components_.T                          # [n_items, k]

        # Normalize for cosine similarity
        if self.config.similarity == "cosine":
            self.user_embeddings = normalize(self.user_embeddings)
            self.item_embeddings = normalize(self.item_embeddings)

        self._build_index()

    def _build_index(self) -> None:
        """Build FAISS index over item embeddings."""
        dim = self.item_embeddings.shape[1]
        embs = self.item_embeddings.astype(np.float32)

        if self.config.index_type == "flat":
            if self.config.similarity == "cosine":
                self.item_index = faiss.IndexFlatIP(dim)  # inner product (cosine after normalize)
            else:
                self.item_index = faiss.IndexFlatL2(dim)
        else:
            quantizer = faiss.IndexFlatIP(dim)
            self.item_index = faiss.IndexIVFFlat(
                quantizer, dim, self.config.n_lists,
                faiss.METRIC_INNER_PRODUCT
            )
            self.item_index.train(embs)
            self.item_index.nprobe = self.config.n_probe

        self.item_index.add(embs)

    def retrieve(
        self,
        user_id,
        exclude_seen: bool = True,
        seen_items: Optional[list] = None,
    ) -> list[tuple]:
        """
        Retrieve top-K candidates for a user.
        Returns: list of (item_id, retrieval_score) sorted descending.
        """
        if user_id not in self.user_enc:
            return self._cold_start()

        user_emb = self.user_embeddings[self.user_enc[user_id]].reshape(1, -1)
        scores, indices = self.item_index.search(
            user_emb.astype(np.float32), self.config.n_candidates + len(seen_items or [])
        )

        candidates = [
            (self.item_ids[idx], float(score))
            for score, idx in zip(scores[0], indices[0])
            if idx < len(self.item_ids)
        ]

        if exclude_seen and seen_items:
            seen_set = set(seen_items)
            candidates = [(iid, s) for iid, s in candidates if iid not in seen_set]

        return candidates[:self.config.n_candidates]

    def _cold_start(self) -> list[tuple]:
        """Return global popularity-based candidates for new users."""
        # Return items with highest average scores in the index
        return [(self.item_ids[i], 1.0) for i in range(min(self.config.n_candidates, len(self.item_ids)))]

    def retrieve_similar_items(self, item_id, top_k: int = 20) -> list[tuple]:
        """Item-to-item retrieval: find similar items."""
        if item_id not in self.item_enc:
            return []
        item_emb = self.item_embeddings[self.item_enc[item_id]].reshape(1, -1)
        scores, indices = self.item_index.search(item_emb.astype(np.float32), top_k + 1)
        return [
            (self.item_ids[idx], float(score))
            for score, idx in zip(scores[0], indices[0])
            if self.item_ids[idx] != item_id
        ][:top_k]
```

---

## Stage 2: Learn-to-Rank (XGBoost LambdaMART)

```python
# ml/recommendations/stage2_ranker.py

import xgboost as xgb
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
import joblib
from typing import Optional
from sklearn.preprocessing import StandardScaler


@dataclass
class RankerConfig:
    n_estimators: int = 500
    max_depth: int = 6
    learning_rate: float = 0.05
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    min_child_weight: int = 5
    objective: str = "rank:ndcg"     # 'rank:ndcg' | 'rank:map' | 'rank:pairwise'
    eval_metric: str = "ndcg@10"
    early_stopping_rounds: int = 50
    n_jobs: int = -1
    feature_names: list = field(default_factory=list)


class LearnToRankModel:
    """
    XGBoost LambdaMART ranker.
    Takes candidates from Stage 1 and re-ranks with rich features.
    """

    def __init__(self, config: RankerConfig):
        self.config = config
        self.model = None
        self.scaler = StandardScaler()

    def build_features(
        self,
        candidates: list[tuple],      # (item_id, retrieval_score)
        user_id,
        users_df: pd.DataFrame,        # user metadata
        items_df: pd.DataFrame,        # item metadata
        interactions_df: pd.DataFrame, # historical interactions
    ) -> pd.DataFrame:
        """
        Construct feature matrix for ranking.
        Features should cover: retrieval score, item stats, user preferences,
        user-item affinity signals, context features.
        """
        rows = []
        user_row = users_df[users_df["user_id"] == user_id].iloc[0] if len(users_df) else {}

        for item_id, retrieval_score in candidates:
            item_row = items_df[items_df["item_id"] == item_id]
            if len(item_row) == 0:
                continue
            item_row = item_row.iloc[0]

            # User-item interaction history
            user_item_hist = interactions_df[
                (interactions_df["user_id"] == user_id) &
                (interactions_df["item_id"] == item_id)
            ]

            # Item global statistics
            item_all_interactions = interactions_df[interactions_df["item_id"] == item_id]

            row = {
                "item_id": item_id,
                # Retrieval signal
                "retrieval_score": retrieval_score,
                # Item statistics
                "item_avg_rating": item_all_interactions["rating"].mean() if len(item_all_interactions) else 0,
                "item_rating_count": len(item_all_interactions),
                "item_rating_std": item_all_interactions["rating"].std() if len(item_all_interactions) > 1 else 0,
                # Add domain-specific features below:
                # Books: genre match, author familiarity, publication year
                # Products: price range affinity, category match, brand preference
                # User features: activity level, avg rating, diversity preference
            }
            rows.append(row)

        return pd.DataFrame(rows)

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,               # relevance labels (0/1 or graded)
        group: np.ndarray,           # query group sizes [n_items_for_query_1, ...]
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
        group_val: Optional[np.ndarray] = None,
    ) -> None:
        X_scaled = self.scaler.fit_transform(X)

        dtrain = xgb.DMatrix(X_scaled, label=y)
        dtrain.set_group(group)

        evals = [(dtrain, "train")]
        if X_val is not None:
            X_val_scaled = self.scaler.transform(X_val)
            dval = xgb.DMatrix(X_val_scaled, label=y_val)
            dval.set_group(group_val)
            evals.append((dval, "val"))

        params = {
            "objective": self.config.objective,
            "eval_metric": self.config.eval_metric,
            "max_depth": self.config.max_depth,
            "learning_rate": self.config.learning_rate,
            "subsample": self.config.subsample,
            "colsample_bytree": self.config.colsample_bytree,
            "min_child_weight": self.config.min_child_weight,
            "nthread": self.config.n_jobs,
        }

        self.model = xgb.train(
            params, dtrain,
            num_boost_round=self.config.n_estimators,
            evals=evals,
            early_stopping_rounds=self.config.early_stopping_rounds,
            verbose_eval=50,
        )

    def predict(self, X: np.ndarray) -> np.ndarray:
        X_scaled = self.scaler.transform(X)
        return self.model.predict(xgb.DMatrix(X_scaled))

    def rank_candidates(
        self, feature_df: pd.DataFrame, top_n: int = 50
    ) -> pd.DataFrame:
        feature_cols = [c for c in feature_df.columns if c != "item_id"]
        scores = self.predict(feature_df[feature_cols].values)
        feature_df = feature_df.copy()
        feature_df["ranker_score"] = scores
        return feature_df.sort_values("ranker_score", ascending=False).head(top_n)

    def save(self, path: str) -> None:
        self.model.save_model(f"{path}.xgb")
        joblib.dump(self.scaler, f"{path}.scaler.pkl")

    def load(self, path: str) -> None:
        self.model = xgb.Booster()
        self.model.load_model(f"{path}.xgb")
        self.scaler = joblib.load(f"{path}.scaler.pkl")
```

---

## Stage 3: Diversity Reranking (MMR)

```python
# ml/recommendations/stage3_reranker.py

import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from dataclasses import dataclass


@dataclass
class RerankerConfig:
    lambda_mmr: float = 0.7        # 0 = max diversity, 1 = max relevance
    top_n: int = 10                # final number of recommendations
    embedding_model: str = "all-MiniLM-L6-v2"


class MMRReranker:
    """
    Maximal Marginal Relevance (Carbonell & Goldstein, 1998).
    Balances relevance (ranker score) vs. diversity (embedding distance).

    At each step, selects the item that maximizes:
        MMR(i) = λ × relevance(i) - (1-λ) × max_similarity(i, selected)
    """

    def __init__(self, config: RerankerConfig):
        self.config = config
        self.embedder = SentenceTransformer(config.embedding_model)

    def rerank(
        self,
        candidates: list[dict],        # [{"item_id": ..., "ranker_score": ..., "text": ...}]
        already_seen: list[str] = None, # exclude items similar to already consumed
    ) -> list[dict]:
        if len(candidates) <= self.config.top_n:
            return candidates

        texts = [c.get("text", c["item_id"]) for c in candidates]
        embeddings = self.embedder.encode(texts, normalize_embeddings=True)
        scores = np.array([c["ranker_score"] for c in candidates])

        # Normalize relevance scores to [0, 1]
        scores = (scores - scores.min()) / (scores.max() - scores.min() + 1e-8)
        sim_matrix = cosine_similarity(embeddings)

        selected_indices = []
        remaining = list(range(len(candidates)))

        while len(selected_indices) < self.config.top_n and remaining:
            if not selected_indices:
                # First item: pick highest relevance
                best = max(remaining, key=lambda i: scores[i])
            else:
                # MMR selection
                best = max(
                    remaining,
                    key=lambda i: (
                        self.config.lambda_mmr * scores[i]
                        - (1 - self.config.lambda_mmr) * max(
                            sim_matrix[i][j] for j in selected_indices
                        )
                    ),
                )
            selected_indices.append(best)
            remaining.remove(best)

        return [candidates[i] for i in selected_indices]


class DPPReranker:
    """
    Determinantal Point Process reranking.
    More principled than MMR for batch diversity — selects
    a subset that maximizes the log-determinant of the kernel matrix.
    Use when: you want probabilistic diversity guarantees.
    """

    def __init__(self, top_n: int = 10, embedding_model: str = "all-MiniLM-L6-v2"):
        self.top_n = top_n
        self.embedder = SentenceTransformer(embedding_model)

    def rerank(self, candidates: list[dict]) -> list[dict]:
        texts = [c.get("text", c["item_id"]) for c in candidates]
        embeddings = self.embedder.encode(texts, normalize_embeddings=True)
        scores = np.array([c["ranker_score"] for c in candidates])
        scores = (scores - scores.min()) / (scores.max() - scores.min() + 1e-8)

        # L = diag(scores) * (embeddings @ embeddings.T) * diag(scores)
        # This is the quality-diversity DPP kernel
        L = np.diag(scores) @ (embeddings @ embeddings.T) @ np.diag(scores)

        selected = self._greedy_dpp(L, self.top_n)
        return [candidates[i] for i in selected]

    def _greedy_dpp(self, L: np.ndarray, k: int) -> list[int]:
        """Greedy MAP inference for DPP."""
        selected = []
        remaining = list(range(L.shape[0]))
        Linv_selected = np.zeros((0, 0))

        for _ in range(k):
            best_idx, best_score = None, -np.inf
            for i in remaining:
                # Marginal gain of adding item i
                if len(selected) == 0:
                    score = np.log(L[i, i] + 1e-10)
                else:
                    e_i = L[selected, i]
                    schur = L[i, i] - e_i @ np.linalg.solve(L[np.ix_(selected, selected)], e_i)
                    score = np.log(max(schur, 1e-10))
                if score > best_score:
                    best_score = score
                    best_idx = i
            if best_idx is None:
                break
            selected.append(best_idx)
            remaining.remove(best_idx)
        return selected
```

---

## Full Pipeline Orchestrator

```python
# ml/recommendations/pipeline.py

from .stage1_candidate_gen import CollaborativeFilteringRetriever, CandidateGenConfig
from .stage2_ranker import LearnToRankModel, RankerConfig
from .stage3_reranker import MMRReranker, RerankerConfig
import pandas as pd


class RecommendationPipeline:
    """
    Full 3-stage recommendation pipeline.
    Initialize once, call recommend() per request.
    """

    def __init__(
        self,
        retriever: CollaborativeFilteringRetriever,
        ranker: LearnToRankModel,
        reranker: MMRReranker,
        users_df: pd.DataFrame,
        items_df: pd.DataFrame,
        interactions_df: pd.DataFrame,
    ):
        self.retriever = retriever
        self.ranker = ranker
        self.reranker = reranker
        self.users_df = users_df
        self.items_df = items_df
        self.interactions_df = interactions_df

    def recommend(
        self,
        user_id,
        n_final: int = 10,
        seen_items: list = None,
        context: dict = None,    # optional: time, location, device
    ) -> list[dict]:
        # Stage 1: Retrieve candidates
        candidates = self.retriever.retrieve(
            user_id, exclude_seen=True, seen_items=seen_items or []
        )

        # Stage 2: Rank with rich features
        feature_df = self.ranker.build_features(
            candidates, user_id,
            self.users_df, self.items_df, self.interactions_df
        )
        ranked_df = self.ranker.rank_candidates(feature_df, top_n=50)

        ranked_candidates = [
            {"item_id": row["item_id"], "ranker_score": row["ranker_score"]}
            for _, row in ranked_df.iterrows()
        ]

        # Stage 3: Diversity reranking
        final = self.reranker.rerank(ranked_candidates)
        return final[:n_final]
```

---

## Domain-Specific Feature Engineering

### Book Recommender
```python
BOOK_FEATURES = {
    # User features
    "user_avg_rating": "mean rating given by user",
    "user_genre_diversity": "entropy of genre distribution in user history",
    "user_read_count": "total books rated",
    "user_author_diversity": "number of unique authors read",
    # Item features
    "book_avg_rating": "global average rating",
    "book_popularity": "log(rating_count)",
    "book_recency": "years since publication (normalized)",
    "book_genre_match": "cosine similarity of book genre to user genre profile",
    # User-item interaction features
    "author_familiarity": "has user rated books by this author before",
    "series_continuation": "is book a sequel to something user read",
}
```

### Skincare Product Recommender
```python
SKINCARE_FEATURES = {
    # User features (from facial detection ML backend)
    "has_dark_circles": "binary — detected by facial analysis model",
    "has_pores": "binary",
    "has_eye_bags": "binary",
    "skin_type": "oily / dry / combination / normal",
    "user_age_bracket": "from profile",
    # Item features
    "targets_dark_circles": "binary — product claims dark circle treatment",
    "targets_pores": "binary",
    "ingredient_match_score": "overlap between user's beneficial ingredients and product",
    "price_tier": "budget / mid / premium",
    "brand_trust_score": "user's historical ratings for this brand",
}
```

---

## Evaluation Metrics

```python
# ml/recommendations/evaluation.py

import numpy as np


def ndcg_at_k(recommended: list, relevant: list, k: int = 10) -> float:
    """Normalized Discounted Cumulative Gain@K."""
    rec_k = recommended[:k]
    dcg = sum(
        1 / np.log2(rank + 2) for rank, item in enumerate(rec_k) if item in relevant
    )
    idcg = sum(1 / np.log2(rank + 2) for rank in range(min(k, len(relevant))))
    return dcg / idcg if idcg > 0 else 0.0


def hit_rate_at_k(recommended: list, relevant: list, k: int = 10) -> float:
    """Fraction of users for whom at least one relevant item appears in top-K."""
    return float(any(item in relevant for item in recommended[:k]))


def intra_list_diversity(recommended: list, embeddings: dict, k: int = 10) -> float:
    """Average pairwise distance in the recommendation list — measures diversity."""
    from sklearn.metrics.pairwise import cosine_similarity
    recs = recommended[:k]
    embs = np.array([embeddings[i] for i in recs if i in embeddings])
    if len(embs) < 2:
        return 0.0
    sim = cosine_similarity(embs)
    n = len(embs)
    return 1 - (sim.sum() - n) / (n * (n - 1))
```

---

## Usage in Claude Code

```bash
# Train full pipeline on book dataset
python -m ml.recommendations.train \
  --users data/books/users.csv \
  --items data/books/books.csv \
  --ratings data/books/ratings.csv \
  --output models/book_recommender/

# Evaluate pipeline
python -m ml.recommendations.evaluate \
  --model models/book_recommender/ \
  --test-users 1000 \
  --metrics ndcg@10 hit_rate@10 diversity

# Serve recommendations
python -m ml.recommendations.serve --model models/book_recommender/ --port 8001

# Tune lambda_mmr for diversity tradeoff
python -m ml.recommendations.tune_diversity --lambda-range 0.3 0.5 0.7 0.9
```
