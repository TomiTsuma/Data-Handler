# skill: nlp-transformers
# Trigger: "BERT", "transformer", "fine-tune", "text classification", "NLP",
#          "sentiment", "named entity", "text generation", "tokenizer", "huggingface",
#          "language model", "probing", "interpretability", "embeddings",
#          "customer feedback", "text analysis", "LLM interpretability"

## Purpose
Covers all NLP work: BERT family fine-tuning, text generation, customer feedback
analysis (Core&Outline module), LLM interpretability probing (PhD research thread),
and corpus processing for Kenyan news data.

## Stack
- HuggingFace Transformers + Datasets + Evaluate
- PyTorch (training backend)
- spaCy (NLP preprocessing)
- Sentence-Transformers (embeddings, semantic search)
- scikit-learn (probing classifiers)

---

## Fine-Tuning BERT for Classification

```python
# ml/nlp/finetune_classifier.py
"""
Standard BERT fine-tuning template for Core&Outline's text classification tasks:
- Sentiment classification (feedback analysis module)
- Topic classification
- Intent detection
"""

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import torch
import numpy as np
from torch.utils.data import Dataset, DataLoader
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    TrainingArguments,
    Trainer,
    DataCollatorWithPadding,
    EarlyStoppingCallback,
)
from datasets import load_dataset, Dataset as HFDataset
import evaluate


@dataclass
class BERTClassifierConfig:
    model_name: str = "bert-base-uncased"       # or "distilbert-base-uncased" for speed
    num_labels: int = 3                          # e.g. positive / negative / neutral
    label_names: list = None                     # ["positive", "negative", "neutral"]
    max_length: int = 256
    batch_size: int = 16
    learning_rate: float = 2e-5
    epochs: int = 5
    warmup_ratio: float = 0.1
    weight_decay: float = 0.01
    output_dir: Path = Path("models/bert_classifier")
    device: str = "cuda" if torch.cuda.is_available() else "cpu"


class TextDataset(Dataset):
    def __init__(self, texts: list[str], labels: list[int], tokenizer, max_length: int):
        self.encodings = tokenizer(
            texts, truncation=True, padding=True,
            max_length=max_length, return_tensors="pt"
        )
        self.labels = labels

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, idx):
        return {
            "input_ids": self.encodings["input_ids"][idx],
            "attention_mask": self.encodings["attention_mask"][idx],
            "labels": torch.tensor(self.labels[idx], dtype=torch.long),
        }


def compute_metrics(eval_pred):
    metric_acc = evaluate.load("accuracy")
    metric_f1 = evaluate.load("f1")
    logits, labels = eval_pred
    preds = np.argmax(logits, axis=-1)
    return {
        **metric_acc.compute(predictions=preds, references=labels),
        **metric_f1.compute(predictions=preds, references=labels, average="weighted"),
    }


def finetune_bert(
    train_texts: list[str],
    train_labels: list[int],
    val_texts: list[str],
    val_labels: list[int],
    config: BERTClassifierConfig,
) -> AutoModelForSequenceClassification:
    tokenizer = AutoTokenizer.from_pretrained(config.model_name)
    model = AutoModelForSequenceClassification.from_pretrained(
        config.model_name, num_labels=config.num_labels
    )

    train_dataset = TextDataset(train_texts, train_labels, tokenizer, config.max_length)
    val_dataset = TextDataset(val_texts, val_labels, tokenizer, config.max_length)

    training_args = TrainingArguments(
        output_dir=str(config.output_dir),
        num_train_epochs=config.epochs,
        per_device_train_batch_size=config.batch_size,
        per_device_eval_batch_size=config.batch_size,
        learning_rate=config.learning_rate,
        warmup_ratio=config.warmup_ratio,
        weight_decay=config.weight_decay,
        evaluation_strategy="epoch",
        save_strategy="epoch",
        load_best_model_at_end=True,
        metric_for_best_model="f1",
        report_to="wandb",
        fp16=torch.cuda.is_available(),
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=val_dataset,
        tokenizer=tokenizer,
        data_collator=DataCollatorWithPadding(tokenizer),
        compute_metrics=compute_metrics,
        callbacks=[EarlyStoppingCallback(early_stopping_patience=2)],
    )
    trainer.train()
    trainer.save_model(str(config.output_dir / "best"))
    return model
```

---

## BERT for Text Generation (Masked LM → Generation)
```python
# ml/nlp/bert_generation.py
"""
BERT-based text generation using iterative masked token prediction.
Note: GPT-family models are better for generation — use BERT generation
only when you need bidirectional context (e.g. controlled infilling tasks).
"""

from transformers import pipeline, AutoModelForMaskedLM, AutoTokenizer
import torch


def bert_infill(
    template: str,          # "The product has [MASK] quality and [MASK] packaging."
    model_name: str = "bert-base-uncased",
    top_k: int = 5,
) -> list[str]:
    """
    Fill [MASK] tokens in a template.
    Use for structured text generation with constraints.
    """
    fill = pipeline("fill-mask", model=model_name, device=0 if torch.cuda.is_available() else -1)
    results = fill(template, top_k=top_k)
    return [r["sequence"] for r in results]


def iterative_generation(
    seed_text: str,
    n_words: int = 20,
    model_name: str = "bert-base-uncased",
) -> str:
    """
    Iterative masked token generation.
    Appends [MASK], predicts, appends prediction, repeats.
    """
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForMaskedLM.from_pretrained(model_name)
    model.eval()

    text = seed_text
    for _ in range(n_words):
        masked = text + " [MASK]"
        inputs = tokenizer(masked, return_tensors="pt")
        with torch.no_grad():
            outputs = model(**inputs)
        mask_idx = (inputs["input_ids"] == tokenizer.mask_token_id).nonzero()[0, 1]
        predicted_token = tokenizer.decode(outputs.logits[0, mask_idx].argmax())
        text += f" {predicted_token}"
    return text
```

---

## Customer Feedback Analysis Pipeline (Core&Outline Module)

```python
# ml/nlp/feedback_analysis.py
"""
Full pipeline for Core&Outline's AI-powered customer feedback analysis module.
Input: Raw feedback texts (surveys, reviews, support tickets)
Output: Sentiment scores, topic clusters, actionable insights, trend signals
"""

from dataclasses import dataclass
from typing import Optional
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans, DBSCAN
from sklearn.preprocessing import normalize
from transformers import pipeline


@dataclass
class FeedbackAnalysisConfig:
    sentiment_model: str = "cardiffnlp/twitter-roberta-base-sentiment-latest"
    embedding_model: str = "all-MiniLM-L6-v2"
    n_topics: int = 8                     # K-means clusters for topic extraction
    min_cluster_size: int = 3             # DBSCAN min_samples
    batch_size: int = 32


class FeedbackAnalysisPipeline:

    def __init__(self, config: FeedbackAnalysisConfig):
        self.config = config
        self.sentiment = pipeline(
            "sentiment-analysis",
            model=config.sentiment_model,
            device=0 if torch.cuda.is_available() else -1,
        )
        self.embedder = SentenceTransformer(config.embedding_model)

    def analyze(self, texts: list[str]) -> dict:
        """Full analysis: sentiment + topics + insights."""
        # 1. Sentiment scoring
        sentiments = self._batch_sentiment(texts)

        # 2. Semantic embeddings for topic clustering
        embeddings = self.embedder.encode(
            texts, batch_size=self.config.batch_size,
            show_progress_bar=True, normalize_embeddings=True
        )

        # 3. Topic clustering
        topics = self._cluster_topics(texts, embeddings)

        # 4. Trend detection (requires time series — pass timestamps separately)
        return {
            "overall_sentiment": np.mean([s["score"] * (1 if s["label"] == "positive" else -1)
                                          for s in sentiments]),
            "sentiment_distribution": self._sentiment_dist(sentiments),
            "topics": topics,
            "embeddings": embeddings,  # for downstream similarity search
        }

    def _batch_sentiment(self, texts: list[str]) -> list[dict]:
        results = []
        for i in range(0, len(texts), self.config.batch_size):
            batch = texts[i:i + self.config.batch_size]
            results.extend(self.sentiment(batch, truncation=True, max_length=512))
        return results

    def _cluster_topics(
        self, texts: list[str], embeddings: np.ndarray
    ) -> list[dict]:
        """K-means topic clustering with representative text extraction."""
        n_clusters = min(self.config.n_topics, len(texts) // 3)
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        labels = kmeans.fit_predict(embeddings)

        topics = []
        for cluster_id in range(n_clusters):
            mask = labels == cluster_id
            cluster_texts = [t for t, m in zip(texts, mask) if m]
            cluster_embs = embeddings[mask]
            centroid = kmeans.cluster_centers_[cluster_id]
            distances = np.linalg.norm(cluster_embs - centroid, axis=1)
            rep_idx = distances.argmin()

            topics.append({
                "topic_id": cluster_id,
                "size": int(mask.sum()),
                "representative_text": cluster_texts[rep_idx],
                "sample_texts": cluster_texts[:3],
            })
        return sorted(topics, key=lambda t: t["size"], reverse=True)

    def _sentiment_dist(self, sentiments: list[dict]) -> dict:
        from collections import Counter
        counts = Counter(s["label"] for s in sentiments)
        total = len(sentiments)
        return {k: v / total for k, v in counts.items()}
```

---

## LLM Interpretability — Probing Classifiers (PhD Research)

```python
# research/interpretability/probing.py
"""
Probing classifier framework for LLM interpretability (PhD primary topic).
Tests whether linguistic properties are encoded in specific layers of LLMs.

Methodology:
1. Extract hidden states from LLM at each layer
2. Train a lightweight linear probe on the hidden states
3. Measure probe accuracy across layers → find where property is encoded
4. Compare across model architectures (GPT-2, LLaMA, Mistral)

Properties to probe: POS tags, syntactic dependencies, semantic roles,
number agreement, negation scope, tense, coreference.
"""

import torch
import numpy as np
from dataclasses import dataclass
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score
from transformers import AutoModel, AutoTokenizer
from typing import Callable


@dataclass
class ProbingConfig:
    model_name: str = "gpt2"
    property_name: str = "pos_tags"          # what linguistic property we're probing
    layers_to_probe: list = None             # None = all layers
    probe_type: str = "linear"               # 'linear' | 'mlp'
    cv_folds: int = 5
    max_length: int = 128
    batch_size: int = 16
    device: str = "cuda" if torch.cuda.is_available() else "cpu"


class HiddenStateExtractor:
    """Extract intermediate representations from any HuggingFace LLM."""

    def __init__(self, model_name: str, device: str = "cpu"):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(
            model_name, output_hidden_states=True
        ).to(device).eval()
        self.device = device

        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token

    @torch.no_grad()
    def extract(
        self,
        texts: list[str],
        layers: list[int] = None,
        aggregation: str = "mean",   # 'mean' | 'cls' | 'last'
        batch_size: int = 16,
    ) -> dict[int, np.ndarray]:
        """
        Returns: {layer_idx: np.ndarray of shape [n_samples, hidden_dim]}
        """
        all_hidden = {}

        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            inputs = self.tokenizer(
                batch, return_tensors="pt", padding=True,
                truncation=True, max_length=512
            ).to(self.device)

            outputs = self.model(**inputs)
            hidden_states = outputs.hidden_states  # tuple of [batch, seq, hidden]

            target_layers = layers if layers else list(range(len(hidden_states)))
            for layer_idx in target_layers:
                h = hidden_states[layer_idx]  # [batch, seq, hidden]
                if aggregation == "mean":
                    # Mean over non-padding tokens
                    mask = inputs["attention_mask"].unsqueeze(-1).float()
                    rep = (h * mask).sum(1) / mask.sum(1)
                elif aggregation == "cls":
                    rep = h[:, 0, :]           # CLS / first token
                elif aggregation == "last":
                    rep = h[:, -1, :]

                rep_np = rep.cpu().float().numpy()
                if layer_idx not in all_hidden:
                    all_hidden[layer_idx] = []
                all_hidden[layer_idx].append(rep_np)

        return {k: np.concatenate(v) for k, v in all_hidden.items()}


class ProbingClassifier:
    """
    Linear/MLP probe trained on hidden states to decode linguistic properties.
    Key insight: high probe accuracy at layer L → property is encoded at layer L.
    """

    def __init__(self, config: ProbingConfig):
        self.config = config
        self.extractor = HiddenStateExtractor(config.model_name, config.device)
        self.probes: dict[int, LogisticRegression] = {}

    def probe_all_layers(
        self,
        texts: list[str],
        labels: list[int],           # property labels per text (e.g. 0=past, 1=present, 2=future)
        layers: list[int] = None,
    ) -> dict[int, float]:
        """
        Train and evaluate one probe per layer.
        Returns: {layer_idx: cross_val_accuracy}
        """
        hidden_by_layer = self.extractor.extract(texts, layers=layers)
        results = {}

        for layer_idx, representations in hidden_by_layer.items():
            probe = LogisticRegression(max_iter=1000, C=1.0, random_state=42)
            scores = cross_val_score(
                probe, representations, labels,
                cv=self.config.cv_folds, scoring="accuracy"
            )
            results[layer_idx] = float(scores.mean())
            print(f"Layer {layer_idx:02d}: accuracy = {scores.mean():.3f} ± {scores.std():.3f}")

        return results

    def find_encoding_layer(self, probing_results: dict[int, float]) -> int:
        """Layer with highest probe accuracy = where property is most encoded."""
        return max(probing_results, key=probing_results.get)

    def compare_architectures(
        self,
        model_names: list[str],
        texts: list[str],
        labels: list[int],
    ) -> dict[str, dict[int, float]]:
        """Compare how different architectures encode the same property."""
        comparison = {}
        for model_name in model_names:
            print(f"\nProbing {model_name}...")
            self.config.model_name = model_name
            self.extractor = HiddenStateExtractor(model_name, self.config.device)
            comparison[model_name] = self.probe_all_layers(texts, labels)
        return comparison
```

---

## Semantic Search (Core&Outline — Query-to-Insight Retrieval)

```python
# ml/nlp/semantic_search.py
"""
Semantic search over business insights, feedback clusters, and metrics.
Used in Core&Outline's AI Business Analyst to retrieve relevant context.
"""

import numpy as np
import faiss
from sentence_transformers import SentenceTransformer


class SemanticSearchIndex:
    """FAISS-backed semantic search over text corpus."""

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self.model = SentenceTransformer(model_name)
        self.index = None
        self.texts = []

    def build(self, texts: list[str]) -> None:
        self.texts = texts
        embeddings = self.model.encode(texts, normalize_embeddings=True)
        dim = embeddings.shape[1]
        self.index = faiss.IndexFlatIP(dim)   # Inner product = cosine similarity (normalized)
        self.index.add(embeddings.astype(np.float32))

    def search(self, query: str, top_k: int = 5) -> list[dict]:
        q_emb = self.model.encode([query], normalize_embeddings=True)
        scores, indices = self.index.search(q_emb.astype(np.float32), top_k)
        return [
            {"text": self.texts[i], "score": float(scores[0][rank])}
            for rank, i in enumerate(indices[0])
            if i < len(self.texts)
        ]

    def save(self, path: str) -> None:
        faiss.write_index(self.index, f"{path}.faiss")
        import json
        with open(f"{path}.texts.json", "w") as f:
            json.dump(self.texts, f)

    def load(self, path: str) -> None:
        self.index = faiss.read_index(f"{path}.faiss")
        import json
        with open(f"{path}.texts.json") as f:
            self.texts = json.load(f)
```

---

## Named Entity Recognition (Kenyan Context)

```python
# ml/nlp/ner.py
"""
NER for Kenyan news corpus — identifies organizations, people, locations,
financial entities, and agriculture-related terms.
Used for: regional trend analysis, news monitoring (Core&Outline module).
"""

import spacy
from transformers import pipeline


def extract_entities_spacy(text: str, model: str = "en_core_web_sm") -> list[dict]:
    nlp = spacy.load(model)
    doc = nlp(text)
    return [
        {"text": ent.text, "label": ent.label_, "start": ent.start_char, "end": ent.end_char}
        for ent in doc.ents
    ]


def extract_entities_bert(text: str) -> list[dict]:
    """More accurate than spaCy for domain-specific entities."""
    ner = pipeline("ner", model="dslim/bert-base-NER", aggregation_strategy="simple")
    return ner(text)


def extract_financial_entities(text: str) -> dict:
    """
    Domain-specific: extract financial figures, company names, percentages.
    Useful for Core&Outline's digital media monitoring module.
    """
    import re
    return {
        "currencies": re.findall(r"(?:KES|USD|EUR|GBP)\s*[\d,]+(?:\.\d+)?", text),
        "percentages": re.findall(r"\d+(?:\.\d+)?%", text),
        "companies": [],   # populated by BERT NER above
    }
```

---

## Text Preprocessing for ML Pipelines

```python
# ml/nlp/preprocessing.py

import re
import string
from typing import Optional


def clean_text(
    text: str,
    lowercase: bool = True,
    remove_urls: bool = True,
    remove_mentions: bool = True,
    remove_hashtags: bool = False,
    remove_punctuation: bool = False,
    strip_whitespace: bool = True,
) -> str:
    if remove_urls:
        text = re.sub(r"http\S+|www\S+", "", text)
    if remove_mentions:
        text = re.sub(r"@\w+", "", text)
    if remove_hashtags:
        text = re.sub(r"#\w+", "", text)
    if lowercase:
        text = text.lower()
    if remove_punctuation:
        text = text.translate(str.maketrans("", "", string.punctuation))
    if strip_whitespace:
        text = re.sub(r"\s+", " ", text).strip()
    return text


def chunk_text(text: str, chunk_size: int = 512, overlap: int = 50) -> list[str]:
    """Split long documents into overlapping chunks for embedding."""
    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size - overlap):
        chunk = " ".join(words[i:i + chunk_size])
        chunks.append(chunk)
        if i + chunk_size >= len(words):
            break
    return chunks
```

---

## Key Models Reference

| Task | Model | Why |
|---|---|---|
| Sentiment (general) | `cardiffnlp/twitter-roberta-base-sentiment-latest` | Trained on social media text |
| Sentiment (reviews) | `nlptown/bert-base-multilingual-uncased-sentiment` | 5-star rating output |
| NER | `dslim/bert-base-NER` | CoNLL-2003 trained, fast |
| Semantic similarity | `all-MiniLM-L6-v2` | Speed + quality balance |
| Long doc embedding | `all-mpnet-base-v2` | Best quality, slower |
| Zero-shot classify | `facebook/bart-large-mnli` | No labels needed |
| Text generation | `gpt2-medium` | Local, controllable |
| Probing (PhD) | `gpt2`, `meta-llama/Llama-2-7b-hf`, `mistralai/Mistral-7B-v0.1` | Architecture comparison |

---

## Usage in Claude Code

```bash
# Fine-tune sentiment classifier on feedback data
python -m ml.nlp.finetune_classifier \
  --data data/feedback/labeled.csv --model bert-base-uncased --epochs 5

# Run full feedback analysis pipeline
python -m ml.nlp.feedback_analysis \
  --input data/feedback/raw.json --output results/feedback_analysis.json

# Probing experiment: where does GPT-2 encode tense?
python -m research.interpretability.probing \
  --model gpt2 --property tense --data research/data/tense_probing.csv

# Compare probing across architectures
python -m research.interpretability.probing \
  --models gpt2 gpt2-medium gpt2-large --property pos_tags

# Build semantic search index for Core&Outline insights
python -m ml.nlp.semantic_search build \
  --corpus data/insights/all_insights.json --output models/search_index
```
