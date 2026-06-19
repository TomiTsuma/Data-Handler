# CLAUDE.md — Tomi's Claude Code Configuration

## Who I Am
I'm Tomi, a Machine Learning Engineer (4 years) based in Kenya. I'm the founder of
**Core&Outline** — a SaaS startup delivering automated data analytics, ML, and AI solutions
to businesses across waste management, fintech, and agritech. I'm also a PhD applicant
(Computer Science, University of Exeter) exploring LLM interpretability and GNN-based
reasoning over tabular data.

---

## Primary Projects

### Core&Outline (Startup — Active Development)
- **Stack**: Python, React, PostgreSQL, Redis, FastAPI
- **ML Stack**: TensorFlow, PyTorch, scikit-learn, XGBoost, PyTorch Geometric
- **Data Stack**: Pandas, PySpark, dbt, Airflow
- **Infra**: Docker, Jenkins CI/CD, AWS (S3, EC2, SageMaker)
- **Key modules**: Customer feedback analysis, session recording pipeline (rrweb + Puppeteer +
  FFmpeg), KVI detection & scoring, dynamic pricing engine, business metric comprehension
  framework, AI Business Analyst (LLM + graph-tabular fusion)

### PhD Research (Active — Exeter Application)
- **Primary topic**: Interpreting language representations in LLMs (Ref: 5555)
- **Secondary research thread**: GNN + LLM fusion for tabular data reasoning
  (drug discovery / molecular property prediction as domain application)
- **Novel architecture**: Graph-tabular fusion + interpretable attention mechanisms
- **Goal**: Answer business queries by treating DataFrames as graphs with adjacency matrices —
  no SQL/RAG, direct structural reasoning

---

## Coding Standards

### Python
```python
# Always use type hints
def predict_churn(features: pd.DataFrame, threshold: float = 0.5) -> pd.Series:
    ...

# Prefer dataclasses or Pydantic for configs
@dataclass
class PricingConfig:
    learning_rate: float = 0.001
    gamma: float = 0.95

# Docstrings: Google style
def score_kvi(item_id: str) -> float:
    """Score a Key Value Item.

    Args:
        item_id: Product identifier.

    Returns:
        Normalized KVI score between 0 and 1.
    """
```

### Project Structure (Core&Outline convention)
```
core_outline/
├── api/           # FastAPI routers
├── ml/
│   ├── models/    # Model definitions
│   ├── pipelines/ # Training & inference pipelines
│   └── features/  # Feature engineering
├── data/
│   ├── ingestion/ # Source connectors
│   ├── cleaning/  # Preprocessing
│   └── graphs/    # Graph construction from tabular data
├── analytics/     # Business metric computations
├── tests/
└── scripts/       # One-off automation, Slack bots, scrapers
```

### React (Frontend)
- Functional components + hooks only
- Tailwind CSS for styling
- ECharts for data visualization
- State: Zustand for global, useState/useReducer for local

---

## Preferred Approaches

| Domain | Preference |
|---|---|
| Recommendation | Candidate gen → XGBoost learn-to-rank → diversity reranking |
| Pricing | Deep Q-learning (DQfD), continuous action spaces |
| NLP | Fine-tuned transformers (BERT family), not prompt-only |
| Graph ML | PyTorch Geometric; GCN/GAT for node classification |
| LLM Integration | Direct API calls (Anthropic), streaming, structured outputs |
| Data pipelines | Airflow DAGs, idempotent tasks, DuckDB for local |
| Testing | pytest, fixtures, parametrize; mock external APIs |
| Version control | Conventional commits; feature branches; PR-based |

---

## Environment
- **OS**: Ubuntu 24
- **Python**: 3.11+
- **Node**: LTS
- **Package manager**: pip (venvs), npm
- **Editor**: VS Code
- **Location timezone**: Africa/Nairobi (EAT, UTC+3)

---

## Communication Style
- Be direct and technical — I'm a senior ML engineer, skip beginner explanations
- Show code first, explain after (unless architecture discussion)
- When suggesting approaches, briefly state the tradeoff
- Flag if something conflicts with my established stack or architecture
- For research topics: cite relevant papers/methods, use correct ML terminology

---

## Things to Avoid
- Don't suggest moving away from my established stack without strong justification
- Don't generate boilerplate without context (ask what module/pipeline it belongs to)
- Don't simplify ML math — I can handle the full formulation
- Avoid generic "best practices" lectures; assume I know them

---

## Financial Context (for automation scripts)
- Monthly income: 127,000 KES
- Loans: 5K/mo + 13K/mo + 55K/mo (mortgage)
- Tennis: 1,000 KES/session (biweekly)
- Commute: 1,200 KES/trip (biweekly)
- Internet: 3,500 KES/month
