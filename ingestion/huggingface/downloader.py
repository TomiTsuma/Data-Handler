from __future__ import annotations

from pathlib import Path
from typing import List
import os
import pandas as pd
from pathlib import Path
from huggingface_hub import hf_hub_download, list_repo_files

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
        workspace = self._prepare_workspace(job)
        files = list_repo_files(
            self.dataset_id,
            repo_type="dataset"
        )
        for file in files:
            file_path = hf_hub_download(
                repo_id=self.dataset_id,
                filename=file,  # example
                repo_type="dataset",
                local_dir=self.download_dir+"/"+job.job_id
            )

        return workspace

    def push_to_minio(self, job: IngestionJob,  workspace: Path) -> List[str]:
        uploaded_objects: List[str] = []
        abs_paths = []
        print("-------------",workspace)
        for root, dirs, files in os.walk(workspace):
            for file in files:
                abs_path = os.path.abspath(os.path.join(root, file))
                abs_paths.append(abs_path)

        for file_path in abs_paths:
            file_path = Path(file_path).resolve()
            print(file_path)
            workspace = Path(workspace).resolve()
            print(workspace)
            relative = file_path.relative_to(workspace)
            object_name = job.destination.object_name(relative)
            upload_file(job.destination.bucket, file_path, f"{self.dataset_id}/{object_name}")
            uploaded_objects.append(object_name)
        return uploaded_objects

    def run(self, job: IngestionJob) -> List[str]:
        logger.info("Executing Huggingface ingestion job %s", job.job_id)
        workspace = self.download_hf_dataset(job)
        # if not file:
        #     logger.warning("No files downloaded for job %s", job.job_id)
        #     return []
        uploaded = self.push_to_minio(job, workspace)
        # logger.info("Completed job %s (%d objects uploaded)", job.job_id, len(uploaded))
        # return uploaded