from __future__ import annotations

import sys
sys.path.append('/home/rhadamanthys/Data-Handler')
print("PYTHON:", sys.executable)
print("PATH:", sys.path)
import argparse
import os
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

from core.models.datasource import KaggleDataSource, ArxivDataSource, HuggingFaceDataSource
from core.models.ingestion_job import Destination, IngestionJob
from infrastructure.logging.logger import get_logger
from ingestion.registry import get_pipeline_for
from services.job_runner import JobRunner

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run an ingestion job")
    group = parser.add_mutually_exclusive_group(required=True)
    parser.add_argument(
        "--source",
        required=True,
        help="Name of the data source eg kaggle, arxiv, huggingface",
    )
    group.add_argument(
        "--job",
        dest="job_name",
        help="Name of the job defined in config/kaggle.yaml",
    )
    group.add_argument(
        "--dataset-id",
        dest="dataset_id",
        help="Direct dataset identifier for example if for Kaggle use the form owner_slug/dataset_slug",
    )
    parser.add_argument(
        "--files",
        nargs="*",
        help="Optional file names (space separated) to upload when using --dataset-id",
    )
    parser.add_argument(
        "--bucket",
        help="MinIO bucket to use with --dataset-id (defaults to MINIO_DEFAULT_BUCKET)",
    )
    parser.add_argument(
        "--prefix",
        default="",
        help="Object prefix to use with --dataset-id uploads",
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        default=Path("data/tmp"),
        help="Temporary directory used for downloads",
    )
    parser.add_argument(
        "--arxiv-category",
        default="cs.LG",
        help="Arxiv category to download papers from (only used with --source arxiv)",
    )
    parser.add_argument(
        "--query-mode",
        choices=["category", "query"],
        default="category",
        help="Query mode: 'category' for arxiv category search, 'query' for full-text search",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=100,
        help="Maximum number of papers to download (default: 100)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=20,
        help="Maximum number of pages to fetch (default: 20)",
    )
    parser.add_argument(
        "--keywords",
        nargs="*",
        help="Keywords to filter arxiv papers (space separated)",
    )
    return parser.parse_args()


def _parse_dataset_id(dataset_id: str) -> tuple[str, str]:
    if "/" not in dataset_id:
        raise ValueError("dataset_id must be in the form owner_slug/dataset_slug")
    owner_slug, dataset_slug = dataset_id.split("/", 1)
    owner_slug = owner_slug.strip()
    dataset_slug = dataset_slug.strip()
    if not owner_slug or not dataset_slug:
        raise ValueError("dataset_id must include both owner and dataset slugs")
    return owner_slug, dataset_slug


def _files_from_args(files: Optional[List[str]]) -> List[str] | None:
    if not files:
        return None
    cleaned = [item.strip() for item in files if item.strip()]
    return cleaned or None


def run_managed_job(job_name: str) -> List[str]:
    runner = JobRunner()
    logger.info("Starting managed job %s", job_name)
    return runner.run(job_name)


def run_ad_hoc_dataset(
    source: str,
    dataset_id: str,
    files: Optional[List[str]],
    bucket: Optional[str],
    prefix: str,
    workspace: Path,
    arxiv_category: str,
    query_mode: str = "category",
    max_results: int = 100,
    max_pages: int = 20,
    keywords: Optional[List[str]] = None
) -> List[str]:
    bucket = bucket or os.getenv("MINIO_DEFAULT_BUCKET")
    if not bucket:
        raise ValueError("MinIO bucket must be provided via --bucket or MINIO_DEFAULT_BUCKET")

    destination = Destination(bucket=bucket, prefix=prefix)

    if source == "kaggle":
        owner_slug, dataset_slug = _parse_dataset_id(dataset_id)
        source_obj = KaggleDataSource(
            owner_slug=owner_slug,
            dataset_slug=dataset_slug,
            file_names=files
        )
    elif source == "arxiv":
        source_obj = ArxivDataSource(
            name=dataset_id,
            category=arxiv_category,
            dataset_slug=dataset_id,
            file_names=files
        )
    elif source == "huggingface":
        source_obj = HuggingFaceDataSource(
            dataset_slug=dataset_id,
            file_names=files
        )
    else:
        raise ValueError(f"Unknown source: {source}")

    job = IngestionJob(
        job_id=dataset_id.replace("/", "-"),
        source=source_obj,
        destination=destination,
        workspace=workspace,
        kind=source
    )

    pipeline = get_pipeline_for(job, arxiv_category=arxiv_category, dataset_id=dataset_id, max_results=max_results, keywords=keywords)
    if hasattr(pipeline, 'downloader') and hasattr(pipeline.downloader, 'query_mode'):
        pipeline.downloader.query_mode = query_mode

    logger.info("Starting ad-hoc ingestion for %s (max_results=%d)", dataset_id, max_results)
    return pipeline.run(job)


def main():
    load_dotenv()
    args = parse_args()

    if args.job_name:
        result = run_managed_job(args.job_name)
    else:
        result = run_ad_hoc_dataset(
            source=args.source,
            dataset_id=args.dataset_id,
            files=_files_from_args(args.files),
            bucket=args.bucket,
            prefix=args.prefix,
            workspace=args.workspace,
            arxiv_category=args.arxiv_category,
            query_mode=args.query_mode,
            max_results=args.max_results,
            max_pages=args.max_pages,
            keywords=_files_from_args(args.keywords)
        )

    logger.info("Ingestion complete: %d objects uploaded", len(result))


if __name__ == "__main__":
    main()