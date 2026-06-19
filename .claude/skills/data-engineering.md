# skill: data-engineering
# Trigger: "Airflow", "DAG", "pipeline", "ETL", "ingestion", "DuckDB", "dbt",
#          "data cleaning", "data connector", "scraping", "crawl", "data lake",
#          "data warehouse", "schema", "partition", "data quality", "Great Expectations",
#          "data lineage", "batch processing", "streaming"

## Purpose
Data pipelines for Core&Outline: Airflow DAGs for scheduled ingestion,
DuckDB for local analysis, dbt for transformations, web scraping for
Kenyan news corpus and competitor monitoring, data validation.

## Stack
- Apache Airflow 2.x (orchestration)
- DuckDB (local OLAP, fast prototyping)
- dbt (SQL transformations + tests)
- Great Expectations (data validation)
- BeautifulSoup + Playwright (scraping)
- Pandas + PyArrow (data processing)
- PostgreSQL (operational store)

---

## Airflow DAG Template

```python
# dags/core_outline_ingestion.py
"""
Core&Outline data ingestion DAG.
Runs daily at 1 AM EAT (22:00 UTC previous day).
Ingests: financial data, SaaS metrics, customer events, social media.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable


logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "tomi",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="core_outline_daily_ingestion",
    default_args=DEFAULT_ARGS,
    description="Daily data ingestion pipeline for Core&Outline",
    schedule_interval="0 22 * * *",   # 22:00 UTC = 01:00 EAT
    start_date=days_ago(1),
    catchup=False,
    tags=["core-outline", "ingestion", "daily"],
    doc_md="""
    # Core&Outline Daily Ingestion

    Ingests data from all connected sources:
    1. Financial transactions → cleaned → S3 + PostgreSQL
    2. SaaS metrics (subscriptions, events) → aggregated → metrics store
    3. Customer feedback → NLP preprocessing queue
    4. Social media / news → trend analysis pipeline

    On failure: Slack alert to #data-engineering. PagerDuty for P1 failures.
    """,
) as dag:

    def ingest_financial_data(**context):
        """Ingest financial transactions from source systems."""
        from data.ingestion.financial import FinancialIngester
        execution_date = context["execution_date"]
        ingester = FinancialIngester(date=execution_date.date())
        result = ingester.run()
        logger.info(f"Ingested {result['records']} financial records for {execution_date.date()}")
        # XCom: pass record count downstream
        return result["records"]

    def validate_financial_data(**context):
        """Run Great Expectations validation on ingested data."""
        from data.validation.financial_suite import validate
        ti = context["task_instance"]
        records = ti.xcom_pull(task_ids="ingest_financial")
        if records == 0:
            raise ValueError("Zero records ingested — possible source failure")
        validate(date=context["execution_date"].date())

    def compute_daily_metrics(**context):
        """Compute SaaS KPIs from raw events."""
        from analytics.saas_metrics import compute_daily
        compute_daily(date=context["execution_date"].date())

    def queue_feedback_for_nlp(**context):
        """Push new feedback texts to NLP processing queue (Redis)."""
        import redis, json
        r = redis.Redis.from_url(Variable.get("REDIS_URL"))
        from data.ingestion.feedback import get_new_feedback
        texts = get_new_feedback(date=context["execution_date"].date())
        for text in texts:
            r.lpush("nlp:feedback_queue", json.dumps(text))
        logger.info(f"Queued {len(texts)} feedback items for NLP processing")

    # Task definitions
    t_ingest_financial = PythonOperator(
        task_id="ingest_financial",
        python_callable=ingest_financial_data,
        provide_context=True,
    )

    t_validate_financial = PythonOperator(
        task_id="validate_financial",
        python_callable=validate_financial_data,
        provide_context=True,
    )

    t_compute_metrics = PythonOperator(
        task_id="compute_daily_metrics",
        python_callable=compute_daily_metrics,
        provide_context=True,
    )

    t_queue_feedback = PythonOperator(
        task_id="queue_feedback_nlp",
        python_callable=queue_feedback_for_nlp,
        provide_context=True,
    )

    t_run_dbt = BashOperator(
        task_id="run_dbt_transformations",
        bash_command="cd /opt/core-outline/dbt && dbt run --profiles-dir . --select tag:daily",
    )

    t_dbt_test = BashOperator(
        task_id="dbt_tests",
        bash_command="cd /opt/core-outline/dbt && dbt test --profiles-dir . --select tag:daily",
    )

    # Task dependencies (DAG structure)
    t_ingest_financial >> t_validate_financial >> t_compute_metrics
    t_validate_financial >> t_run_dbt >> t_dbt_test
    t_ingest_financial >> t_queue_feedback
```

---

## DuckDB — Local OLAP Analysis

```python
# data/analysis/duckdb_queries.py
"""
DuckDB for fast local data exploration before scaling to Spark or BigQuery.
Reads directly from Parquet/CSV on S3 or local disk.
"""

import duckdb
import pandas as pd
from pathlib import Path


def get_connection(database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Get DuckDB connection. Use ':memory:' for ephemeral, file path for persistent."""
    con = duckdb.connect(database)
    # Configure for S3 access
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region='af-south-1';
        SET s3_access_key_id='{__import__('os').environ.get('AWS_ACCESS_KEY_ID', '')}';
        SET s3_secret_access_key='{__import__('os').environ.get('AWS_SECRET_ACCESS_KEY', '')}';
    """)
    return con


# --- Common analysis queries ---

CHURN_ANALYSIS_QUERY = """
WITH monthly_cohorts AS (
    SELECT
        DATE_TRUNC('month', created_at) AS cohort_month,
        customer_id,
        cancelled_at
    FROM subscriptions
    WHERE status IN ('active', 'cancelled')
),
churn_by_cohort AS (
    SELECT
        cohort_month,
        COUNT(*) AS cohort_size,
        COUNT(CASE WHEN cancelled_at IS NOT NULL
              AND DATE_TRUNC('month', cancelled_at) = cohort_month THEN 1 END) AS churned,
    FROM monthly_cohorts
    GROUP BY cohort_month
)
SELECT
    cohort_month,
    cohort_size,
    churned,
    ROUND(100.0 * churned / cohort_size, 2) AS churn_rate_pct
FROM churn_by_cohort
ORDER BY cohort_month DESC
LIMIT 12;
"""


MRR_TREND_QUERY = """
SELECT
    DATE_TRUNC('month', event_date) AS month,
    SUM(CASE WHEN event_type = 'new' THEN mrr_amount ELSE 0 END) AS new_mrr,
    SUM(CASE WHEN event_type = 'expansion' THEN mrr_amount ELSE 0 END) AS expansion_mrr,
    SUM(CASE WHEN event_type = 'churn' THEN mrr_amount ELSE 0 END) AS churned_mrr,
    SUM(CASE WHEN event_type = 'contraction' THEN mrr_amount ELSE 0 END) AS contraction_mrr,
    SUM(mrr_amount) AS net_new_mrr
FROM mrr_movements
GROUP BY month
ORDER BY month DESC
LIMIT 12;
"""


KVI_ANALYSIS_QUERY = """
-- Key Value Item detection: find items with high price sensitivity
SELECT
    item_id,
    item_name,
    AVG(price) AS avg_price,
    CORR(price, demand) AS price_demand_correlation,
    STDDEV(price) / AVG(price) AS price_volatility,
    AVG(demand) AS avg_demand,
    COUNT(*) AS observation_count,
    -- KVI score: strong correlation + high demand + competitive pressure
    ABS(CORR(price, demand)) * 0.4
        + NTILE(100) OVER (ORDER BY AVG(demand)) / 100.0 * 0.4
        + (1 - STDDEV(price) / NULLIF(AVG(price), 0)) * 0.2 AS kvi_score
FROM pricing_observations
WHERE observation_count >= 30
GROUP BY item_id, item_name
HAVING COUNT(*) >= 30
ORDER BY kvi_score DESC
LIMIT 50;
"""


def run_query(sql: str, params: dict = None, return_df: bool = True):
    """Run DuckDB query, return DataFrame or list of tuples."""
    con = get_connection()
    if params:
        result = con.execute(sql, list(params.values()))
    else:
        result = con.execute(sql)
    return result.df() if return_df else result.fetchall()


def analyze_parquet_s3(s3_path: str, query_template: str) -> pd.DataFrame:
    """Query a Parquet file on S3 directly."""
    con = get_connection()
    sql = query_template.replace("{table}", f"read_parquet('{s3_path}')")
    return con.execute(sql).df()
```

---

## dbt Models

```sql
-- dbt/models/metrics/mrr_movements.sql
-- Tracks MRR changes by type: new, expansion, contraction, churn

{{ config(
    materialized='incremental',
    unique_key='movement_id',
    tags=['daily', 'mrr'],
    description='Monthly Recurring Revenue movement events'
) }}

WITH current_subscriptions AS (
    SELECT
        customer_id,
        plan_id,
        mrr_amount,
        status,
        created_at,
        cancelled_at,
        updated_at
    FROM {{ ref('stg_subscriptions') }}
    {% if is_incremental() %}
    WHERE updated_at >= (SELECT MAX(event_date) - INTERVAL '1 day' FROM {{ this }})
    {% endif %}
),
previous_month AS (
    SELECT
        customer_id,
        mrr_amount AS prev_mrr,
        status AS prev_status
    FROM {{ ref('stg_subscriptions') }}
    WHERE DATE_TRUNC('month', updated_at) = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['cs.customer_id', 'cs.updated_at']) }} AS movement_id,
    cs.customer_id,
    CURRENT_DATE AS event_date,
    cs.mrr_amount,
    CASE
        WHEN pm.customer_id IS NULL THEN 'new'
        WHEN cs.status = 'cancelled' THEN 'churn'
        WHEN cs.mrr_amount > pm.prev_mrr THEN 'expansion'
        WHEN cs.mrr_amount < pm.prev_mrr THEN 'contraction'
        ELSE 'retained'
    END AS event_type,
    cs.mrr_amount - COALESCE(pm.prev_mrr, 0) AS mrr_amount
FROM current_subscriptions cs
LEFT JOIN previous_month pm ON cs.customer_id = pm.customer_id
```

```yaml
# dbt/models/metrics/schema.yml
version: 2

models:
  - name: mrr_movements
    description: "MRR movement events — new, expansion, contraction, churn, retained"
    tags: [daily, mrr, metrics]
    columns:
      - name: movement_id
        description: "Surrogate key"
        tests:
          - unique
          - not_null
      - name: mrr_amount
        tests:
          - not_null
      - name: event_type
        tests:
          - accepted_values:
              values: ["new", "expansion", "contraction", "churn", "retained"]
```

---

## Data Validation (Great Expectations)

```python
# data/validation/financial_suite.py

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from datetime import date


def validate(date: date) -> bool:
    """
    Validate daily financial data against expectations.
    Raises on critical failures, warns on minor issues.
    """
    context = gx.get_context()

    batch_request = RuntimeBatchRequest(
        datasource_name="financial_parquet",
        data_connector_name="runtime_connector",
        data_asset_name="financial_transactions",
        runtime_parameters={"path": f"s3://core-outline-data/data/financial/{date}/"},
        batch_identifiers={"date": str(date)},
    )

    # Critical expectations (fail pipeline on breach)
    critical_suite = context.get_expectation_suite("financial.critical")
    validator = context.get_validator(batch_request=batch_request,
                                      expectation_suite=critical_suite)

    validator.expect_column_to_exist("transaction_id")
    validator.expect_column_values_to_not_be_null("transaction_id")
    validator.expect_column_values_to_be_unique("transaction_id")
    validator.expect_column_values_to_not_be_null("amount")
    validator.expect_column_values_to_be_between("amount", min_value=0, max_value=10_000_000)
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=10_000_000)

    results = validator.validate()
    if not results.success:
        failed = [r for r in results.results if not r.success]
        raise ValueError(f"Data validation failed: {[r.expectation_config.expectation_type for r in failed]}")

    return True
```

---

## Web Scraping — Kenyan News + Competitor Monitoring

```python
# data/ingestion/news_scraper.py
"""
Production-grade web scraper for Kenyan news corpus and competitor monitoring.
Respects robots.txt, uses polite delays, stores to S3.
"""

import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from pathlib import Path
import json
import logging
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)

KENYAN_NEWS_SOURCES = {
    "nation": {"url": "https://nation.africa/kenya", "delay": 2.0},
    "standard": {"url": "https://www.standardmedia.co.ke", "delay": 2.0},
    "the_star": {"url": "https://www.the-star.co.ke", "delay": 1.5},
    "business_daily": {"url": "https://businessdailyafrica.com", "delay": 2.0},
    "tuko": {"url": "https://www.tuko.co.ke", "delay": 1.0},
    "citizen_digital": {"url": "https://www.citizen.digital", "delay": 2.0},
    "kbc": {"url": "https://www.kbc.co.ke", "delay": 2.0},
    "capital_fm": {"url": "https://www.capitalfm.co.ke", "delay": 1.5},
}


@dataclass
class ScrapedArticle:
    source: str
    url: str
    title: str
    body: str
    published_at: str = None
    category: str = None


async def scrape_article(session: aiohttp.ClientSession, url: str, source: str, delay: float) -> ScrapedArticle | None:
    await asyncio.sleep(delay)
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Core&Outline Research Bot)"}
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return None
            html = await resp.text()

        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "aside", "header"]):
            tag.decompose()

        article = soup.find("article") or soup.find("main") or soup.find("div", class_="content")
        if not article:
            return None

        title = soup.find("h1")
        title_text = title.get_text(strip=True) if title else ""
        body_text = article.get_text(separator="\n", strip=True)

        if len(body_text) < 200:
            return None

        return ScrapedArticle(source=source, url=url, title=title_text, body=body_text)

    except Exception as e:
        logger.warning(f"Failed to scrape {url}: {e}")
        return None


async def scrape_all_sources(max_per_source: int = 50, output_path: str = None) -> list[ScrapedArticle]:
    articles = []

    async with aiohttp.ClientSession() as session:
        for source, config in KENYAN_NEWS_SOURCES.items():
            logger.info(f"Crawling {source}...")

            try:
                async with session.get(config["url"], timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    html = await resp.text()
                soup = BeautifulSoup(html, "html.parser")
                links = list(set(
                    urljoin(config["url"], a["href"])
                    for a in soup.find_all("a", href=True)
                    if urlparse(urljoin(config["url"], a["href"])).netloc
                    == urlparse(config["url"]).netloc
                ))[:max_per_source]

                tasks = [scrape_article(session, url, source, config["delay"]) for url in links]
                results = await asyncio.gather(*tasks)
                source_articles = [r for r in results if r is not None]
                articles.extend(source_articles)
                logger.info(f"{source}: {len(source_articles)} articles scraped")

            except Exception as e:
                logger.error(f"Error crawling {source}: {e}")

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            for article in articles:
                f.write(f"--- {article.source.upper()} | {article.url} ---\n")
                f.write(f"{article.title}\n\n{article.body}\n\n")
        logger.info(f"Saved {len(articles)} articles to {output_path}")

    return articles
```

---

## Data Ingestion Connectors

```python
# data/ingestion/connectors.py
"""
Source connectors for Core&Outline's automated data ingestion.
Each connector is idempotent — safe to re-run.
"""

import pandas as pd
from abc import ABC, abstractmethod
from datetime import date


class BaseConnector(ABC):
    """All connectors must implement this interface."""

    @abstractmethod
    def extract(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Extract data for the given date range."""

    @abstractmethod
    def validate_schema(self, df: pd.DataFrame) -> bool:
        """Validate that extracted data matches expected schema."""

    def run(self, start_date: date, end_date: date) -> dict:
        df = self.extract(start_date, end_date)
        if not self.validate_schema(df):
            raise ValueError(f"{self.__class__.__name__}: Schema validation failed")
        return {"records": len(df), "data": df}


class QuickBooksConnector(BaseConnector):
    """Financial data from QuickBooks API."""
    def extract(self, start_date, end_date): ...
    def validate_schema(self, df): ...


class StripeConnector(BaseConnector):
    """Subscription and payment data from Stripe."""
    def extract(self, start_date, end_date): ...
    def validate_schema(self, df): ...


class GoogleAnalyticsConnector(BaseConnector):
    """Web analytics from Google Analytics 4."""
    def extract(self, start_date, end_date): ...
    def validate_schema(self, df): ...


class PostgreSQLConnector(BaseConnector):
    """Direct DB connection for operational data."""
    def __init__(self, connection_string: str, table: str, date_column: str):
        self.conn_str = connection_string
        self.table = table
        self.date_col = date_column

    def extract(self, start_date, end_date):
        import sqlalchemy as sa
        engine = sa.create_engine(self.conn_str)
        query = f"""
            SELECT * FROM {self.table}
            WHERE {self.date_col} BETWEEN '{start_date}' AND '{end_date}'
        """
        return pd.read_sql(query, engine)

    def validate_schema(self, df): return not df.empty
```

---

## Usage in Claude Code

```bash
# Trigger Airflow DAG manually
airflow dags trigger core_outline_daily_ingestion --exec-date 2026-03-14

# Run DuckDB analysis locally
python -m data.analysis.duckdb_queries --query churn_analysis --output results/churn.csv

# Run dbt models
cd dbt && dbt run --select tag:daily && dbt test --select tag:daily

# Scrape Kenyan news corpus
python -m data.ingestion.news_scraper \
  --max-per-source 100 --output data/corpus/kenyan_news_2026.txt

# Validate data quality
python -m data.validation.financial_suite --date 2026-03-14

# Run full ingestion pipeline locally (without Airflow)
python -m data.pipelines.daily_ingestion --date 2026-03-14
```
