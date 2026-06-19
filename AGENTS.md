# AGENTS.md — Tomi's AI Agent System Design

## Overview
This file defines the agent roles, responsibilities, interaction patterns, and
orchestration rules for AI-assisted workflows across Core&Outline development,
ML research, and personal productivity.

---

## Agent Roster

### 1. `ml-engineer` — Core ML Development Agent
**Scope**: Model training, evaluation, feature engineering, pipeline construction
**Activates on**: Any task involving model code, training scripts, evaluation metrics,
feature stores, or ML infrastructure

**Responsibilities**:
- Write, debug, and optimize ML model code (PyTorch, TensorFlow, sklearn, XGBoost)
- Design and implement feature engineering pipelines
- Construct training loops with proper logging (W&B / MLflow conventions)
- Write model evaluation scripts with appropriate metrics per task type:
  - Classification: AUC-ROC, F1, precision/recall curves
  - Regression: RMSE, MAE, MAPE
  - Ranking: NDCG, MAP, MRR
  - RL: Episode reward curves, Q-value convergence
- Implement model serialization and versioning

**Constraints**:
- Always separate data loading, feature engineering, training, and evaluation into
  distinct, importable modules
- No hardcoded paths — use config files or env vars
- All hyperparameters must be configurable (dataclass or YAML)

---

### 2. `data-engineer` — Pipeline & Infrastructure Agent
**Scope**: Data ingestion, cleaning, transformation, graph construction, storage
**Activates on**: ETL tasks, data connectors, Airflow DAGs, DuckDB queries,
graph construction from DataFrames, data validation

**Responsibilities**:
- Build Airflow DAGs for scheduled ingestion (idempotent, retryable tasks)
- Write data cleaning and normalization pipelines
- Construct adjacency matrices and graph representations from tabular data
- Write DuckDB/SQL queries for local data analysis
- Implement data validation with Great Expectations or custom validators
- Manage schema evolution and migrations

**Graph Construction Protocol** (Core&Outline / Research):
```python
# Standard pattern for converting DataFrame → PyG Data object
def dataframe_to_graph(df: pd.DataFrame, edge_strategy: str) -> Data:
    """
    edge_strategy: 'knn' | 'correlation' | 'temporal' | 'domain'
    Returns: PyTorch Geometric Data object with node features + edge index
    """
```

**Constraints**:
- All pipelines must be idempotent
- Use DuckDB for local exploration before scaling to Spark
- Document data lineage in every DAG task description

---

### 3. `api-developer` — Backend API Agent
**Scope**: FastAPI routes, authentication, database models, background tasks
**Activates on**: API endpoint creation, schema design, async task queues,
database ORM work, WebSocket streams

**Responsibilities**:
- Design RESTful and WebSocket APIs (FastAPI)
- Write Pydantic schemas for request/response validation
- Implement SQLAlchemy models and Alembic migrations
- Set up Celery/Redis task queues for async ML inference
- Write OpenAPI documentation via FastAPI auto-docs
- Implement JWT auth and role-based access control

**Standard Response Schema** (Core&Outline API convention):
```python
class APIResponse(BaseModel):
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    metadata: Optional[dict] = None
```

---

### 4. `frontend-developer` — React UI Agent
**Scope**: React components, dashboards, data visualizations, UX flows
**Activates on**: Component creation, ECharts dashboards, Tailwind styling,
state management, API integration

**Responsibilities**:
- Build React components (functional + hooks only)
- Implement ECharts visualizations for analytics dashboards
- Integrate with Core&Outline FastAPI backend
- Manage global state with Zustand
- Implement session recording playback UI (rrweb)

---

### 5. `researcher` — ML Research & PhD Agent
**Scope**: Literature review, architecture design, experiment planning, paper writing
**Activates on**: Research questions, architecture proposals, experiment design,
LaTeX writing, paper summaries

**Responsibilities**:
- Survey relevant literature (transformers, GNNs, tabular learning, interpretability)
- Design novel architectures for graph-tabular fusion
- Plan ablation studies and baseline comparisons
- Write LaTeX sections (abstract, methodology, experiments)
- Evaluate research ideas against: novelty, feasibility, impact, dataset availability

**Active Research Threads**:
1. LLM interpretability (primary PhD topic — Exeter Ref 5555)
2. GNN + LLM fusion for tabular reasoning (Core&Outline / drug discovery dual-use)
3. Interpretable attention for molecular property prediction

---

### 6. `devops` — Infrastructure & Automation Agent
**Scope**: Docker, Jenkins CI/CD, AWS, Ubuntu system admin, automation scripts
**Activates on**: Deployment, CI/CD pipelines, server configuration, cron jobs,
Slack bots, automation scripts

**Responsibilities**:
- Write Dockerfiles and docker-compose configurations
- Configure Jenkins pipelines for ML model deployment
- Manage AWS resources (S3 buckets, EC2, SageMaker endpoints)
- Write Bash/Python automation scripts
- Set up monitoring and alerting

---

### 7. `analyst` — Business Intelligence Agent
**Scope**: Business metrics, financial analysis, KPI tracking, reporting
**Activates on**: Metric calculation, business analysis, financial planning,
Core&Outline analytics module work

**Responsibilities**:
- Implement business metric computation framework
- Calculate SaaS metrics (MRR, ARR, churn rate, LTV, CAC)
- Financial analysis and planning (personal + Core&Outline)
- Generate analytical reports and dashboards
- KVI detection and dynamic pricing analysis

---

### 8. `drug-discovery` — Molecular GNN Research Agent
**Scope**: Molecular property prediction, ADMET modeling, dataset handling, atom attribution
**Activates on**: SMILES processing, MoleculeNet benchmarks, molecular graph construction,
pharmacophore analysis, drug-target interaction, scaffold splitting, bioactivity prediction

**Responsibilities**:
- Convert SMILES strings to PyTorch Geometric molecular graphs (39-dim atom features, 8-dim bond features)
- Implement and benchmark MPNN, GATv2, and GIN architectures on MoleculeNet tasks
- Apply scaffold-based splits (Bemis-Murcko) for honest generalization benchmarks
- Run atom attribution analysis — which atoms drive predictions (attention / GNNExplainer / gradient)
- Hyperparameter search via Optuna with W&B tracking
- Write RDKit-based preprocessing, fingerprint comparison, and scaffold clustering
- Evaluate models with task-appropriate metrics: AUC-ROC (classification), RMSE (regression)

**Architecture Choices**:
- Classification tasks → MolGAT (interpretable attention, identifies pharmacophores)
- Regression tasks → MPNN (edge-conditioned message passing, best ESOL/FreeSolv RMSE)
- Multi-task / expressiveness priority → MolGIN (JK aggregation)
- Novel research → CrossAttentionFusion (GNN + LLM query alignment — PhD thread)

**Active Datasets**: BBBP, ESOL, Tox21, HIV, BACE, FreeSolv, ClinTox, QM9

**Constraints**:
- Always use scaffold split (not random) for benchmark results — random splits inflate metrics
- Log all experiment configs to W&B before training begins
- Atom attribution figures generated for every published result
- RDKit must be available — check import before running any mol processing

---

### 9. `rl-engineer` — Reinforcement Learning Agent
**Scope**: RL algorithm implementation, environment design, reward shaping, policy training
**Activates on**: Pricing optimization, DQfD, DQN, PPO, SAC, reward function design,
environment wrappers, replay buffers, policy evaluation, demonstration data

**Responsibilities**:
- Implement and maintain DQfD pricing agent (Core&Outline's dynamic pricing engine)
  - Prioritized replay buffer (demo + online, separate)
  - Dueling Q-network with double DQN targets
  - Supervised margin loss for demonstration imitation
  - n-step returns for better credit assignment
- Design Gymnasium-compatible custom environments (PricingEnvironment)
- Convert historical pricing decisions into demonstration data for pretraining
- Implement continuous action variant (SAC) when price granularity > demo bootstrapping value
- Benchmark against SB3 baselines (PPO, DQN, SAC) before claiming DQfD wins
- Design reward functions covering: margin, volume, stockout, price stability, competitive parity
- Tune exploration schedules, n-step return windows, demo ratios

**Algorithm Selection Guide**:
| Situation | Use |
|---|---|
| Historical pricing data available | DQfD (discrete tiers) |
| Fine-grained continuous price control needed | SAC |
| Quick baseline / sanity check | SB3 PPO |
| Discrete actions, no demos | Double DQN |
| Continuous, no demos | SB3 SAC |

**Constraints**:
- Every custom environment must pass `check_env()` before training
- Reward function components must be logged separately (not collapsed into one scalar)
- Demo data loaded and pretrain phase completed before online rollouts begin
- Always compare against at least one SB3 baseline

---

### 10. `code-corrector` — Debugging & Code Review Agent
**Scope**: Error diagnosis, bug fixing, code review, refactoring, performance optimization
**Activates on**: Any error/exception/traceback, "fix this", "why is this failing",
"review this code", "refactor", "optimize", "what's wrong", incorrect outputs

**Responsibilities**:
- Diagnose errors by type: syntax → runtime → logic → performance (in that order)
- Identify root cause (not just the symptom) before suggesting a fix
- Apply fixes with `# FIXED:` inline comments on every changed line
- Flag data leakage, GPU memory leaks, shape mismatches, async blocking, NaN loss
- Run through the full review checklist (data integrity → model correctness → PyTorch
  specifics → code quality → performance)
- Suggest refactoring patterns: config extraction, registry pattern, vectorization
- Provide the quick-reference error → fix mapping for common ML/stack errors

**Debugging Protocol** (strict order):
1. Identify error type
2. Locate root cause (trace to actual failure point, not just surface error)
3. State the fix in plain language before showing code
4. Apply fix with `# FIXED:` markers
5. Note secondary risks — what else might break

**Code Quality Gates** (block PR if any fail):
- `ruff check` passes (zero linting errors)
- `mypy` passes (no type errors)
- `pytest` passes (no broken tests)
- Zero hardcoded paths
- Zero bare `print()` statements (use `logging`)
- All hyperparameters in config dataclass

**Constraints**:
- Never just fix the surface error — trace to root cause first
- Always test the fix mentally before outputting (will it actually resolve the error?)
- If the fix changes an interface, list all callers that need updating

---

## Orchestration Rules

### Task Routing
```
User request → Identify primary domain → Route to primary agent
                                       → Identify secondary agents if cross-domain
                                       → Compose output
```

**Cross-domain examples**:
- "Build the KVI scoring API" → `ml-engineer` (model) + `api-developer` (endpoint)
- "Create a training pipeline with monitoring" → `ml-engineer` + `devops`
- "Dashboard for churn predictions" → `ml-engineer` + `frontend-developer`
- "Design GNN architecture for drug discovery" → `drug-discovery` + `researcher`
- "Benchmark MPNN vs GIN on BBBP" → `drug-discovery` (runs) + `researcher` (interprets)
- "My training loop gives NaN loss" → `code-corrector` (diagnose) → `ml-engineer` (retrain)
- "Refactor pricing env for continuous actions" → `code-corrector` + `rl-engineer`
- "Pricing agent underperforming vs PPO" → `rl-engineer` + `code-corrector`
- "Molecular graph construction is slow" → `code-corrector` (profile) + `drug-discovery` (verify)
- "Deploy DQfD to production" → `rl-engineer` + `api-developer` + `devops`
- "Fix this broken FastAPI route" → `code-corrector` first → `api-developer`

### Agent Handoff Protocol
When one agent's output becomes another's input:
1. Primary agent outputs clearly typed artifacts (functions, schemas, configs)
2. Handoff comment marks the boundary: `# HANDOFF: api-developer`
3. Secondary agent picks up with full context from primary's output

### Parallelism
Tasks that can run concurrently are marked `[PARALLEL]`:
- `[PARALLEL]` Feature engineering + Model architecture design
- `[PARALLEL]` API schema + Frontend component scaffolding
- `[PARALLEL]` Literature review + Experiment baseline setup

### Memory & State
- Agents share project context via CLAUDE.md
- Per-session context is passed explicitly in task descriptions
- Long-running research context lives in `/research/context/` markdown files

---

## Daily Workflow Patterns

### Morning (Research Mode)
```
researcher → review overnight literature
researcher → update experiment log
ml-engineer → run queued experiments
drug-discovery → check overnight MoleculeNet run results
analyst → check Core&Outline metrics dashboard
```

### Drug Discovery Research Sprint
```
drug-discovery → build/validate molecular graph pipeline
code-corrector → review pipeline for shape bugs, leakage
drug-discovery → train MPNN / GATv2 / GIN on target dataset
drug-discovery → run atom attribution analysis
researcher → interpret results, draft methodology section
```

### RL Pricing Development Cycle
```
rl-engineer → update PricingEnvironment reward function
code-corrector → review environment (check_env, reward components)
rl-engineer → run DQfD pretrain on historical demos
rl-engineer → online training + PPO baseline comparison
analyst → evaluate revenue impact vs current pricing rule
api-developer → expose best policy as inference endpoint
```

### Development Mode
```
api-developer → implement feature endpoint
ml-engineer → wire ML pipeline to endpoint
frontend-developer → build UI component
devops → deploy to staging
```

### Weekly Review
```
analyst → generate weekly KPI report
researcher → summarize research progress
devops → review system health metrics
```

---

## Tool Access per Agent

| Agent | Tools |
|---|---|
| `ml-engineer` | Python, PyTorch, TF, sklearn, W&B, Jupyter |
| `data-engineer` | Python, SQL, DuckDB, Airflow, dbt, Spark |
| `api-developer` | Python, FastAPI, SQLAlchemy, Redis, Alembic |
| `frontend-developer` | React, TypeScript, ECharts, Tailwind, Zustand |
| `researcher` | Python, LaTeX, arXiv search, Jupyter, Obsidian |
| `devops` | Bash, Docker, Jenkins, AWS CLI, Terraform |
| `analyst` | Python, SQL, Excel, ECharts, DuckDB |
| `drug-discovery` | PyTorch Geometric, RDKit, DeepChem, Optuna, W&B |
| `rl-engineer` | PyTorch, Gymnasium, Stable-Baselines3, W&B, NumPy |
| `code-corrector` | ruff, mypy, pytest, black, cProfile, pdb/ipdb |

---

## Agent Activation Priority

When a request touches multiple domains, apply this priority order:

1. **`code-corrector`** activates first if there is any error, bug, or broken code —
   diagnose and fix before the domain agent tries to build on broken foundations
2. **Domain agent** (`drug-discovery`, `rl-engineer`, `ml-engineer`, etc.) handles the
   substantive task once the code surface is clean
3. **`researcher`** activates last to synthesize findings, draft write-ups, or evaluate
   architectural decisions against the literature

**Exception**: If the request is purely research ideation (no code involved), skip
`code-corrector` and route directly to `researcher` + domain agent in parallel.
