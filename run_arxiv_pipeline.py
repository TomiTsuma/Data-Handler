#!/usr/bin/env python3
"""Run arxiv pipeline to download papers matching specific keywords."""

from core.models.datasource import ArxivDataSource, DataSource
from core.models.ingestion_job import IngestionJob, Destination
from ingestion.registry import get_pipeline_for
from core.utils.file_utils import ensure_dir
from pathlib import Path
import os

# Configuration
CATEGORY = "cs"  # Computer Science papers
DATASET_SLUG = "de_novo_diffusion_molecule_2022_2026"
KEYWORDS = ["de novo", "diffusion", "molecule"]
QUERY_MODE = "query"
MAX_RESULTS = 100

def main():
    # Create workspace directory
    workspace = Path("data/tmp/arxiv")
    ensure_dir(workspace)

    # Create arxiv data source
    source = ArxivDataSource(
        name="arxiv_papers",
        category=CATEGORY,
        dataset_slug=DATASET_SLUG,
        keywords=KEYWORDS,
        query_mode=QUERY_MODE
    )

    # Create ingestion job
    job = IngestionJob(
        job_id=f"arxiv_{DATASET_SLUG}",
        source=source,
        destination=Destination(bucket="arxiv-papers"),
        kind="arxiv"
    )

    # Get pipeline and run
    pipeline = get_pipeline_for(
        job=job,
        arxiv_category=CATEGORY,
        dataset_id=DATASET_SLUG,
        query_mode=QUERY_MODE,
        max_results=MAX_RESULTS,
        keywords=KEYWORDS
    )

    print(f"Running arxiv pipeline...")
    print(f"Category: {CATEGORY}")
    print(f"Keywords: {KEYWORDS}")
    print(f"Query mode: {QUERY_MODE}")
    print(f"Max results: {MAX_RESULTS}")
    print("-" * 50)

    uploaded_files = pipeline.run(job)

    print("-" * 50)
    print(f"Pipeline completed. Uploaded {len(uploaded_files)} files:")
    for f in uploaded_files:
        print(f"  - {f}")

if __name__ == "__main__":
    main()
