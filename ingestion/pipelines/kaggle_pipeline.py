from __future__ import annotations

from typing import Any, List

from core.models.datasource import KaggleDataSource
from core.models.ingestion_job import IngestionJob
from ingestion.kaggle.downloader import KaggleDatasetDownloader
from ingestion.pipelines.base_pipeline import BasePipeline


class KagglePipeline(BasePipeline):
    def __init__(self, downloader: KaggleDatasetDownloader | None = None) -> None:
        self.downloader = downloader or KaggleDatasetDownloader()

    def can_handle(self, job: IngestionJob) -> bool:
        return isinstance(job.source, KaggleDataSource)

    def run(self, job: IngestionJob) -> List[str]:
        return self.downloader.run(job)
