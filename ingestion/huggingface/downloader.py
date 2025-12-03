from __future__ import annotations

from pathlib import Path
from typing import List
import os
from datasets import load_dataset
from pathlib import Path

from core.exceptions.ingestion_error import IngestionError
from core.models.datasource import HuggingFaceDataSource
from core.models.ingestion_job import IngestionJob
from core.utils.file_utils import clean_dir, ensure_dir
from infrastructure.logging.logger import get_logger
from infrastructure.minio.uploader import upload_file

import feedparser
import requests
import time
import fitz # PyMuPDF
import io
from minio import Minio
from minio.error import S3Error
import PyPDF2

logger = get_logger(__name__)


class HuggingFaceDownloader:
    OUTPUT_DIR = "huggingface"

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    def __init__(self,  dataset_id, download_dir="data/tmp", batch_size=1000):
        self.download_dir = download_dir
        self.batch_size = batch_size
        self.dataset_id = dataset_id
        os.makedirs(download_dir, exist_ok=True)

    def _prepare_workspace(self, job: IngestionJob) -> Path:
        workspace = job.workspace_path() / job.job_id
        ensure_dir(workspace)
        clean_dir(workspace)
        return workspace

    def _assert_huggingface_source(self, job: IngestionJob) -> HuggingFaceDownloader:
        if not isinstance(job.source, HuggingFaceDownloader):
            raise IngestionError("HuggingFaceDownloader requires a HuggingFaceDownloader")
        return job.source

    def download_hf_dataset(self, job: IngestionJob):
        Path(data_dir).mkdir(parents=True, exist_ok=True)

        ds = load_dataset(
            path=self.dataset_id,
        )

        output_path = os.path.join(
            "./data/tmp/",{job.job_id}, f"/{dataset_name.replace('/', '_')}_{split}.arrow"
        )
        ds.save_to_disk(output_path)

        return output_path

    def push_to_minio(self, job: IngestionJob, files: List[Path], workspace: Path) -> List[str]:
        uploaded_objects: List[str] = []
        for file_path in files:
            file_path = Path(file_path).resolve()
            workspace = Path(workspace).resolve()
            relative = file_path.relative_to(workspace)
            object_name = job.destination.object_name(relative)
            upload_file(job.destination.bucket, file_path, object_name)
            uploaded_objects.append(object_name)
        return uploaded_objects

    def run(self, job: IngestionJob) -> List[str]:
        logger.info("Executing Huggingface ingestion job %s", job.job_id)
        files, workspace = self.download_hf_dataset(job)
        if not files:
            logger.warning("No files downloaded for job %s", job.job_id)
            return []
        uploaded = self.push_to_minio(job, files, workspace)
        logger.info("Completed job %s (%d objects uploaded)", job.job_id, len(uploaded))
        return uploaded