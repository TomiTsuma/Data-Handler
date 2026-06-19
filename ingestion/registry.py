from __future__ import annotations

from typing import List

from core.models.ingestion_job import IngestionJob
from ingestion.pipelines.base_pipeline import BasePipeline
from ingestion.pipelines.kaggle_pipeline import KagglePipeline
from ingestion.pipelines.hf_pipeline import HuggingFacePipeline
from ingestion.pipelines.arxiv_pipeline import ArxivPipeline


def get_pipeline_for(job: IngestionJob, arxiv_category = None, dataset_id = None, query_mode = "category", max_results = 100, keywords = None) -> BasePipeline:
    # Build ArxivPipeline only if job is arxiv type
    arxiv_pipeline = None
    if job.kind == "arxiv":
        arxiv_pipeline = ArxivPipeline(
            query=arxiv_category or "cs.LG",
            dataset_id=dataset_id or job.source.dataset_slug,
            query_mode=query_mode,
            max_results=max_results,
            keywords=keywords
        )
    _PIPELINES: List[BasePipeline] = [KagglePipeline(), arxiv_pipeline, HuggingFacePipeline(dataset_id=dataset_id)]
    for pipeline in _PIPELINES:
        if pipeline and pipeline.can_handle(job):
            return pipeline
    raise ValueError(f"No pipeline registered for job {job.job_id}")
