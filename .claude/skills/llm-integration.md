# skill: llm-integration
# Trigger: "Anthropic API", "Claude API", "LLM", "language model", "prompt",
#          "streaming", "structured output", "AI analyst", "AI business analyst",
#          "business metric comprehension", "natural language query",
#          "tool use", "function calling", "system prompt", "token", "context window"

## Purpose
All Anthropic API usage patterns: streaming, structured outputs, tool use,
multi-turn conversations, and the AI Business Analyst that sits at the core
of Core&Outline's natural language query interface.

## Stack
- `anthropic` Python SDK (always latest)
- Pydantic (structured output parsing)
- FastAPI (async streaming endpoints)

---

## Base Client Setup

```python
# ml/llm/client.py

import os
from anthropic import Anthropic, AsyncAnthropic
from functools import lru_cache

MODEL = "claude-opus-4-20250514"           # default to Opus for complex reasoning
MODEL_FAST = "claude-haiku-4-5-20251001"   # for high-throughput, latency-sensitive


@lru_cache(maxsize=1)
def get_client() -> Anthropic:
    return Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])


@lru_cache(maxsize=1)
def get_async_client() -> AsyncAnthropic:
    return AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
```

---

## Streaming (FastAPI endpoint)

```python
# api/routers/ai_analyst.py
"""
Streaming response for Core&Outline's AI Business Analyst.
User asks a natural language question → answer streams back in real-time.
"""

import json
import asyncio
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from anthropic import AsyncAnthropic
from ..dependencies import get_current_user, get_db

router = APIRouter(prefix="/ai/analyst", tags=["ai-analyst"])


ANALYST_SYSTEM_PROMPT = """You are an expert AI Business Analyst embedded in Core&Outline,
a data analytics platform. You have access to the user's business data through tool calls.

Your capabilities:
- Answer questions about business metrics (MRR, churn, LTV, CAC, ARPU)
- Analyze customer segments and cohorts
- Identify trends and anomalies in time series data
- Compare performance across time periods, geographies, or product lines
- Suggest actionable insights based on data patterns

When answering:
1. Always ground your response in the actual data (use tool calls to fetch it)
2. Lead with the direct answer, then provide supporting evidence
3. Quantify everything — avoid vague statements
4. Flag data quality issues or unusual patterns
5. Response format: executive summary (2 sentences) → key findings → recommended actions

Current business context: {business_context}
"""


async def stream_analyst_response(
    question: str,
    business_id: str,
    conversation_history: list[dict],
    client: AsyncAnthropic,
):
    """Generator for streaming analyst response."""
    messages = conversation_history + [{"role": "user", "content": question}]

    async with client.messages.stream(
        model=MODEL,
        max_tokens=2048,
        system=ANALYST_SYSTEM_PROMPT.format(business_context=f"business_id={business_id}"),
        messages=messages,
        tools=ANALYST_TOOLS,
    ) as stream:
        async for event in stream:
            if hasattr(event, "type"):
                if event.type == "content_block_delta":
                    if hasattr(event.delta, "text"):
                        yield f"data: {json.dumps({'type': 'text', 'content': event.delta.text})}\n\n"
                elif event.type == "message_stop":
                    yield f"data: {json.dumps({'type': 'done'})}\n\n"


@router.post("/query")
async def query_analyst(
    question: str,
    business_id: str,
    conversation_id: str = None,
    current_user=Depends(get_current_user),
):
    client = get_async_client()
    return StreamingResponse(
        stream_analyst_response(question, business_id, [], client),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
```

---

## Tool Use (Function Calling)

```python
# ml/llm/analyst_tools.py
"""
Tools that the AI Business Analyst can call to fetch live data.
Each tool maps to a Core&Outline analytics function.
"""

ANALYST_TOOLS = [
    {
        "name": "get_metric",
        "description": "Fetch a specific business metric for a given time period. "
                       "Use this for MRR, ARR, churn rate, LTV, CAC, ARPU, NRR, DAU, MAU.",
        "input_schema": {
            "type": "object",
            "properties": {
                "metric": {
                    "type": "string",
                    "enum": ["mrr", "arr", "churn_rate", "ltv", "cac", "arpu", "nrr", "dau", "mau"],
                },
                "start_date": {"type": "string", "format": "date"},
                "end_date": {"type": "string", "format": "date"},
                "segment": {
                    "type": "string",
                    "description": "Optional segment filter: plan_tier, geography, cohort",
                },
            },
            "required": ["metric", "start_date", "end_date"],
        },
    },
    {
        "name": "compare_periods",
        "description": "Compare a metric between two time periods (e.g. 'Compare sales in April vs May').",
        "input_schema": {
            "type": "object",
            "properties": {
                "metric": {"type": "string"},
                "period_a": {
                    "type": "object",
                    "properties": {"start": {"type": "string"}, "end": {"type": "string"}},
                },
                "period_b": {
                    "type": "object",
                    "properties": {"start": {"type": "string"}, "end": {"type": "string"}},
                },
            },
            "required": ["metric", "period_a", "period_b"],
        },
    },
    {
        "name": "get_customer_segment",
        "description": "Fetch characteristics and metrics for a customer segment.",
        "input_schema": {
            "type": "object",
            "properties": {
                "segment_type": {"type": "string", "enum": ["churned", "high_value", "at_risk", "new"]},
                "limit": {"type": "integer", "default": 100},
            },
            "required": ["segment_type"],
        },
    },
    {
        "name": "run_sql_query",
        "description": "Run a read-only SQL query on the business data warehouse. "
                       "Use only when other tools don't cover the required data.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "default": 1000},
            },
            "required": ["query"],
        },
    },
]


def execute_tool_call(tool_name: str, tool_input: dict, business_id: str) -> str:
    """Execute a tool call and return stringified result."""
    from ..analytics import saas_metrics, customer_segments

    if tool_name == "get_metric":
        result = saas_metrics.get_metric(
            business_id=business_id,
            metric=tool_input["metric"],
            start_date=tool_input["start_date"],
            end_date=tool_input["end_date"],
            segment=tool_input.get("segment"),
        )
    elif tool_name == "compare_periods":
        result = saas_metrics.compare_periods(
            business_id=business_id, **tool_input
        )
    elif tool_name == "get_customer_segment":
        result = customer_segments.get_segment(
            business_id=business_id, **tool_input
        )
    elif tool_name == "run_sql_query":
        result = saas_metrics.run_readonly_query(
            business_id=business_id, **tool_input
        )
    else:
        result = {"error": f"Unknown tool: {tool_name}"}

    import json
    return json.dumps(result, default=str)
```

---

## Agentic Loop (Tool Use + Multi-Turn)

```python
# ml/llm/agentic_loop.py
"""
Full agentic loop: Claude reasons, calls tools, gets results, reasons again.
Used by AI Business Analyst for complex multi-step queries.
"""

from anthropic import Anthropic
import json


def run_analyst_agent(
    question: str,
    business_id: str,
    system_prompt: str,
    tools: list[dict],
    max_turns: int = 10,
    model: str = "claude-opus-4-20250514",
) -> str:
    """
    Runs a complete agentic loop until Claude returns stop_reason='end_turn'.
    Returns the final text answer.
    """
    client = Anthropic()
    messages = [{"role": "user", "content": question}]

    for turn in range(max_turns):
        response = client.messages.create(
            model=model,
            max_tokens=4096,
            system=system_prompt,
            tools=tools,
            messages=messages,
        )

        # Append assistant's response to history
        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            # Extract final text
            text_blocks = [b.text for b in response.content if hasattr(b, "text")]
            return "\n".join(text_blocks)

        if response.stop_reason == "tool_use":
            # Execute all requested tool calls
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result = execute_tool_call(block.name, block.input, business_id)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })

            # Append tool results and continue
            messages.append({"role": "user", "content": tool_results})

    return "Max turns reached without completing the task."
```

---

## Structured Output Extraction

```python
# ml/llm/structured_output.py
"""
Force Claude to return structured JSON that maps to Pydantic models.
Used for: business metric comprehension, insight extraction, report generation.
"""

from anthropic import Anthropic
from pydantic import BaseModel, Field
from typing import Optional, Any
import json


def extract_structured(
    prompt: str,
    output_schema: type[BaseModel],
    system: str = "You are a precise data extraction assistant. Always respond with valid JSON only.",
    model: str = "claude-opus-4-20250514",
    max_retries: int = 3,
) -> BaseModel:
    """
    Extract structured data conforming to a Pydantic model.
    Retries on parse failure with error feedback to the model.
    """
    client = Anthropic()
    schema_json = json.dumps(output_schema.model_json_schema(), indent=2)

    full_prompt = f"""{prompt}

Respond ONLY with a JSON object conforming to this schema. No explanation, no markdown:
{schema_json}"""

    messages = [{"role": "user", "content": full_prompt}]

    for attempt in range(max_retries):
        response = client.messages.create(
            model=model,
            max_tokens=2048,
            system=system,
            messages=messages,
        )
        raw = response.content[0].text.strip()

        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

        try:
            return output_schema.model_validate_json(raw)
        except Exception as e:
            if attempt < max_retries - 1:
                messages.append({"role": "assistant", "content": raw})
                messages.append({
                    "role": "user",
                    "content": f"Parse error: {e}. Fix the JSON and return it again."
                })

    raise ValueError(f"Failed to parse structured output after {max_retries} attempts")


# Example Pydantic models for Core&Outline use cases
class MetricComprehensionResult(BaseModel):
    metric_name: str
    formula: str
    required_columns: list[str]
    column_mapping: dict[str, str]   # formula variable → actual column name
    calculated_value: float
    unit: str
    interpretation: str
    confidence: float = Field(ge=0.0, le=1.0)


class FeedbackInsight(BaseModel):
    sentiment: str                   # positive / negative / neutral
    topics: list[str]
    urgency: str                     # low / medium / high
    actionable_recommendations: list[str]
    key_quotes: list[str]


class TrendAnalysis(BaseModel):
    region: str
    topic: str
    trend_direction: str             # rising / falling / stable
    confidence_score: float
    supporting_evidence: list[str]
    relevant_date_range: str
    data_sources: list[str]
```

---

## Prompt Engineering Patterns

```python
# ml/llm/prompts.py
"""
Reusable prompt templates for Core&Outline's AI features.
All prompts use f-strings — inject context before sending.
"""

BUSINESS_ANALYST_SYSTEM = """You are an expert AI Business Analyst.
Expertise: SaaS metrics, customer analytics, financial analysis, data interpretation.
Style: Direct, quantitative, actionable. Lead with the answer. Use numbers, not vague language.
Format: Executive summary → Key findings (bullet list) → Recommendations.
Data source: Core&Outline platform metrics. Always call tools to fetch actual data."""


METRIC_COMPREHENSION_PROMPT = """
Given this business metric and dataset, calculate the metric and explain the result.

Metric: {metric_name}
Definition: {definition}
Dataset sample:
{data_sample}

Instructions:
1. Map dataset columns to the metric's formula variables
2. Calculate the metric value
3. Interpret the result in plain business language
4. Flag any data quality issues

Return as JSON per the provided schema.
"""


REGIONAL_TREND_PROMPT = """
Analyze news articles from {region} for the topic: {topic}.

Articles:
{articles}

Identify:
1. Is interest in {topic} rising, falling, or stable?
2. What are the key drivers?
3. What are the top 3 data points or quotes supporting this trend?
4. What business implications does this trend suggest?

Return as JSON per the provided schema.
"""


CHURN_RISK_EXPLANATION_PROMPT = """
A customer has been flagged as HIGH CHURN RISK by our ML model (score: {churn_score:.2f}).

Customer profile:
{customer_profile}

Recent behavior signals:
{behavior_signals}

Model feature importances:
{feature_importances}

Write a concise (3-5 sentence) plain-English explanation of WHY this customer is at risk,
suitable for a customer success manager to read before a retention call.
Do not use ML jargon. Focus on observable business signals.
"""


FEW_SHOT_METRIC_EXAMPLES = """
Example 1:
Question: What was our churn rate in March?
Available data: subscribers table with columns: customer_id, status, created_at, cancelled_at
Calculation: customers cancelled in March / customers at start of March = 145 / 2300 = 6.3%
Answer: Your churn rate in March was 6.3%, which is above the healthy SaaS benchmark of <5%.

Example 2:
Question: What is our LTV?
Available data: subscriptions table with avg_monthly_revenue=45, churn_rate=0.04
Calculation: LTV = ARPU / monthly_churn_rate = 45 / 0.04 = KES 1,125
Answer: Customer Lifetime Value is KES 1,125. At your current CAC of KES 280, your LTV:CAC ratio is 4:1, which is healthy.
"""
```

---

## Caching & Cost Management

```python
# ml/llm/cache.py
"""
Cache LLM responses to avoid redundant API calls.
Essential for Core&Outline where the same metrics are queried repeatedly.
"""

import hashlib
import json
import redis
from functools import wraps


redis_client = redis.Redis(host="localhost", port=6379, db=1, decode_responses=True)
DEFAULT_TTL = 3600   # 1 hour cache for business metric answers


def cache_llm_response(ttl: int = DEFAULT_TTL):
    """Decorator to cache LLM responses by prompt hash."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = "llm:" + hashlib.sha256(
                json.dumps({"args": str(args), "kwargs": str(kwargs)}).encode()
            ).hexdigest()[:16]

            cached = redis_client.get(cache_key)
            if cached:
                return json.loads(cached)

            result = func(*args, **kwargs)
            redis_client.setex(cache_key, ttl, json.dumps(result, default=str))
            return result
        return wrapper
    return decorator


def estimate_cost(prompt_tokens: int, completion_tokens: int, model: str) -> float:
    """Estimate API cost in USD."""
    rates = {
        "claude-opus-4-20250514": {"input": 15.0, "output": 75.0},   # per 1M tokens
        "claude-sonnet-4-20250514": {"input": 3.0, "output": 15.0},
        "claude-haiku-4-5-20251001": {"input": 0.25, "output": 1.25},
    }
    r = rates.get(model, rates["claude-opus-4-20250514"])
    return (prompt_tokens * r["input"] + completion_tokens * r["output"]) / 1_000_000
```

---

## Usage in Claude Code

```bash
# Test AI analyst with a business question
python -m ml.llm.analyst \
  --question "What was our churn rate last month and what's driving it?" \
  --business-id biz_001

# Extract structured metric comprehension
python -m ml.llm.structured_output \
  --metric "net_revenue_retention" \
  --data data/subscriptions.csv

# Run agentic loop for multi-step analysis
python -m ml.llm.agentic_loop \
  --question "Compare customer acquisition costs in Q1 vs Q2 and recommend optimizations" \
  --business-id biz_001 --max-turns 8

# Benchmark model quality vs cost for analyst queries
python -m ml.llm.benchmark --queries data/test_questions.json --models opus sonnet haiku
```
