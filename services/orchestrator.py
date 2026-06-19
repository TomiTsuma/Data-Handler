from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv

from core.models.datasource import KaggleDataSource, ArxivDataSource
from core.models.ingestion_job import Destination, IngestionJob
from ingestion.registry import get_pipeline_for

load_dotenv()


class IngestionOrchestrator:
    def __init__(self, config_path: Path = Path("config/kaggle.yaml")) -> None:
        self.config_path = config_path
        self._config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        with self.config_path.open("r", encoding="utf-8") as fp:
            return yaml.safe_load(fp)

    def build_job(self, job_name: str, workspace: Path | None = None) -> IngestionJob:
        jobs_cfg = self._config.get("jobs", {})
        job_cfg = jobs_cfg.get(job_name)
        if not job_cfg:
            raise ValueError(f"Job '{job_name}' not defined in {self.config_path}")

        dataset_cfg = job_cfg["dataset"]
        destination_cfg = job_cfg.get("destination", {})
        default_bucket = self._config.get("default_bucket", "")
        destination = Destination(
            bucket=destination_cfg.get("bucket", default_bucket),
            prefix=destination_cfg.get("prefix", ""),
        )

        # Determine job type from dataset config
        if "owner_slug" in dataset_cfg:
            # Kaggle job
            source = KaggleDataSource(
                name=f"kaggle::{dataset_cfg['owner_slug']}/{dataset_cfg['dataset_slug']}",
                owner_slug=dataset_cfg["owner_slug"],
                dataset_slug=dataset_cfg["dataset_slug"],
                file_names=dataset_cfg.get("file_names"),
            )
            kind = "kaggle"
        elif "category" in dataset_cfg:
            # Arxiv job
            source = ArxivDataSource(
                name=dataset_cfg.get("category", "cs.LG"),
                category=dataset_cfg.get("category", "cs.LG"),
                dataset_slug=dataset_cfg.get("dataset_slug", job_name),
                file_names=dataset_cfg.get("file_names"),
                keywords=dataset_cfg.get("keywords"),
                query_mode=dataset_cfg.get("query_mode", "category"),
            )
            kind = "arxiv"
        else:
            raise ValueError(f"Unknown dataset type for job '{job_name}'")

        return IngestionJob(
            job_id=job_name,
            source=source,
            destination=destination,
            workspace=workspace or Path("data/tmp"),
            kind=kind,
        )

    def run(self, job_name: str) -> Any:
        job = self.build_job(job_name)
        # Extract arxiv-specific config from job source
        arxiv_category = None
        query_mode = "category"
        keywords = None
        if isinstance(job.source, ArxivDataSource):
            arxiv_category = job.source.category
            query_mode = job.source.query_mode
            keywords = list(job.source.keywords) if job.source.keywords else None
        pipeline = get_pipeline_for(
            job,
            arxiv_category=arxiv_category,
            dataset_id=job.source.dataset_slug,
            query_mode=query_mode,
            max_results=self._config.get("jobs", {}).get(job_name, {}).get("max_results", 100),
            keywords=keywords
        )
        return pipeline.run(job)

