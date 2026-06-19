from __future__ import annotations

from pathlib import Path
from typing import List

from core.exceptions.ingestion_error import IngestionError
from core.models.datasource import KaggleDataSource
from core.models.ingestion_job import IngestionJob
from core.utils.file_utils import clean_dir, ensure_dir
from infrastructure.logging.logger import get_logger
from infrastructure.minio.uploader import upload_file
from ingestion.kaggle.client import KaggleClient

logger = get_logger(__name__)


class KaggleDatasetDownloader:
    def __init__(self, client: KaggleClient | None = None) -> None:
        self.client = client or KaggleClient()

    def _prepare_workspace(self, job: IngestionJob) -> Path:
        workspace = job.workspace_path() / job.job_id
        ensure_dir(workspace)
        clean_dir(workspace)
        return workspace

    def _assert_kaggle_source(self, job: IngestionJob) -> KaggleDataSource:
        if not isinstance(job.source, KaggleDataSource):
            raise IngestionError("KaggleDatasetDownloader requires a KaggleDataSource")
        return job.source

    def download(self, job: IngestionJob) -> tuple[List[Path], Path]:
        source = self._assert_kaggle_source(job)
        workspace = self._prepare_workspace(job)
        files = self.client.download_dataset(
            owner_slug=source.owner_slug,
            dataset_slug=source.dataset_slug,
            destination=workspace,
            file_names=source.files_to_pull(),
        )
        return files, workspace

    def push_to_minio(self, job: IngestionJob, files: List[Path], workspace: Path) -> List[str]:
        uploaded_objects: List[str] = []
        for file_path in files:
            relative = file_path.relative_to(workspace)
            object_name = job.destination.object_name(relative)
            upload_file(job.destination.bucket, file_path, object_name)
            uploaded_objects.append(object_name)
        return uploaded_objects

    def run(self, job: IngestionJob) -> List[str]:
        logger.info("Executing Kaggle ingestion job %s", job.job_id)
        files, workspace = self.download(job)
        if not files:
            logger.warning("No files downloaded for job %s", job.job_id)
            return []
        uploaded = self.push_to_minio(job, files, workspace)
        logger.info("Completed job %s (%d objects uploaded)", job.job_id, len(uploaded))
        return uploaded
