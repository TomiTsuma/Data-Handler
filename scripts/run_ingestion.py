from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

from core.models.datasource import KaggleDataSource
from core.models.ingestion_job import Destination, IngestionJob
from infrastructure.logging.logger import get_logger
from ingestion.registry import get_pipeline_for
from services.job_runner import JobRunner

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a Kaggle ingestion job")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--job",
        dest="job_name",
        help="Name of the job defined in config/kaggle.yaml",
    )
    group.add_argument(
        "--dataset-id",
        dest="dataset_id",
        help="Direct Kaggle dataset identifier in the form owner_slug/dataset_slug",
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
    dataset_id: str,
    files: Optional[List[str]],
    bucket: Optional[str],
    prefix: str,
    workspace: Path,
) -> List[str]:
    owner_slug, dataset_slug = _parse_dataset_id(dataset_id)
    bucket = bucket or os.getenv("MINIO_DEFAULT_BUCKET")
    if not bucket:
        raise ValueError("MinIO bucket must be provided via --bucket or MINIO_DEFAULT_BUCKET")

    source = KaggleDataSource(
        name=f"kaggle::{dataset_id}",
        owner_slug=owner_slug,
        dataset_slug=dataset_slug,
        file_names=_files_from_args(files),
    )
    destination = Destination(bucket=bucket, prefix=prefix)
    job = IngestionJob(
        job_id=f"kaggle::{owner_slug}::{dataset_slug}",
        source=source,
        destination=destination,
        workspace=workspace,
    )
    logger.info("Starting ad-hoc dataset download for %s", dataset_id)
    pipeline = get_pipeline_for(job)
    return pipeline.run(job)


def main() -> None:
    load_dotenv()
    args = parse_args()
    if args.dataset_id:
        uploaded_objects = run_ad_hoc_dataset(
            dataset_id=args.dataset_id,
            files=args.files,
            bucket=args.bucket,
            prefix=args.prefix,
            workspace=args.workspace,
        )
    else:
        uploaded_objects = run_managed_job(args.job_name)
    logger.info("Uploaded objects: %s", uploaded_objects)


if __name__ == "__main__":
    main()

