#!/usr/bin/env python3
"""Script to run Arxiv ingestion job for de novo diffusion papers."""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.models.datasource import ArxivDataSource
from core.models.ingestion_job import Destination
from core.models.ingestion_job import IngestionJob

# Create ArxivDataSource with keywords and query_mode
source = ArxivDataSource(
    name="de_novo_diffusion",
    category="cs.LG",
    dataset_slug="de_novo_diffusion",
    file_names=None,
    keywords=["de novo", "diffusion"],
    query_mode="query"
)

# Create destination
destination = Destination(
    bucket="research-pdfs",
    prefix="arxiv/de_novo_diffusion"
)

# Create job
job = IngestionJob(
    job_id="arxiv_de_novo_diffusion",
    source=source,
    destination=destination,
    workspace=Path("data/tmp"),
    kind="arxiv"
)

# Run the pipeline
from ingestion.registry import get_pipeline_for

pipeline = get_pipeline_for(
    job,
    arxiv_category=source.category,
    dataset_id=source.dataset_slug,
    query_mode=source.query_mode,
    max_results=100,
    keywords=source.keywords
)

print(f"Running Arxiv job: {job.job_id}")
print(f"Category: {source.category}")
print(f"Query mode: {source.query_mode}")
print(f"Keywords: {source.keywords}")
print(f"Destination: {destination.bucket}/{destination.prefix}")
print()

uploaded = pipeline.run(job)
print(f"\nCompleted! Uploaded {len(uploaded)} papers to Minio.")
for obj in uploaded:
    print(f"  - {obj}")
