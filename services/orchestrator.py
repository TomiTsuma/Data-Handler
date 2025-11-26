from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv

from core.models.datasource import KaggleDataSource
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
        source = KaggleDataSource(
            name=f"kaggle::{dataset_cfg['owner_slug']}/{dataset_cfg['dataset_slug']}",
            owner_slug=dataset_cfg["owner_slug"],
            dataset_slug=dataset_cfg["dataset_slug"],
            file_names=dataset_cfg.get("file_names"),
        )
        return IngestionJob(
            job_id=job_name,
            source=source,
            destination=destination,
            workspace=workspace or Path("data/tmp"),
        )

    def run(self, job_name: str) -> Any:
        job = self.build_job(job_name)
        pipeline = get_pipeline_for(job)
        return pipeline.run(job)

