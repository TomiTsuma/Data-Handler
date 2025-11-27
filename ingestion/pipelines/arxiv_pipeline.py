from __future__ import annotations

from typing import Any, List

from core.models.datasource import ArxivDataSource
from core.models.ingestion_job import IngestionJob
from ingestion.arxiv.downloader import ArxivDownloader
from ingestion.pipelines.base_pipeline import BasePipeline


class ArxivPipeline(BasePipeline):
    def __init__(self, query, dataset_id, downloader: ArxivDownloader | None = None) -> None:
        self.downloader = downloader or ArxivDownloader(arxiv_category=query, dataset_id=dataset_id)

    def can_handle(self, job: IngestionJob) -> bool:
        return isinstance(job.source, ArxivDataSource)

    def run(self, job: IngestionJob) -> List[str]:
        return self.downloader.run(job)
