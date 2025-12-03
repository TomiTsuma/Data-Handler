from __future__ import annotations

from typing import Any, List

from core.models.datasource import HuggingFaceDataSource
from core.models.ingestion_job import IngestionJob
from ingestion.huggingface.downloader import HuggingFaceDownloader
from ingestion.pipelines.base_pipeline import BasePipeline


class HuggingFacePipeline(BasePipeline):
    def __init__(self,  dataset_id, downloader: HuggingFaceDownloader | None = None) -> None:
        self.downloader = downloader or HuggingFaceDownloader(dataset_id=dataset_id)

    def can_handle(self, job: IngestionJob) -> bool:
        return isinstance(job.source, HuggingFaceDataSource)

    def run(self, job: IngestionJob) -> List[str]:
        return self.downloader.run(job)
