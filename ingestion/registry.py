from __future__ import annotations

from typing import List

from core.models.ingestion_job import IngestionJob
from ingestion.pipelines.base_pipeline import BasePipeline
from ingestion.pipelines.kaggle_pipeline import KagglePipeline

_PIPELINES: List[BasePipeline] = [KagglePipeline()]


def get_pipeline_for(job: IngestionJob) -> BasePipeline:
    for pipeline in _PIPELINES:
        if pipeline.can_handle(job):
            return pipeline
    raise ValueError(f"No pipeline registered for job {job.job_id}")
