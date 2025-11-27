from __future__ import annotations

from pathlib import Path
from typing import List

from core.exceptions.ingestion_error import IngestionError
from core.models.datasource import ArxivDataSource
from core.models.ingestion_job import IngestionJob
from core.utils.file_utils import clean_dir, ensure_dir
from infrastructure.logging.logger import get_logger
from infrastructure.minio.uploader import upload_file

import feedparser
import requests
import time
import os
import fitz # PyMuPDF
import io
from minio import Minio
from minio.error import S3Error
import PyPDF2

logger = get_logger(__name__)


class ArxivDownloader:
    CATEGORY = "cs.LG"
    MAX_RESULTS = 10          # arXiv allows up to 30k with multiple calls
    OUTPUT_DIR = "arxiv_papers"
    BASE_URL = "http://export.arxiv.org/api/query?search_query=cat:{}&start={}&max_results={}"

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    def __init__(self, arxiv_category, dataset_id, download_dir="data/tmp", batch_size=10):
        self.query = arxiv_category
        self.download_dir = download_dir
        self.batch_size = batch_size
        self.dataset_id = dataset_id
        os.makedirs(download_dir, exist_ok=True)

    def _prepare_workspace(self, job: IngestionJob) -> Path:
        workspace = job.workspace_path() / job.job_id
        ensure_dir(workspace)
        clean_dir(workspace)
        return workspace

    def _assert_kaggle_source(self, job: IngestionJob) -> ArxivDataSource:
        if not isinstance(job.source, ArxivDataSource):
            raise IngestionError("ArxivDownloader requires a ArxivDataSource")
        return job.source

    def fetch_papers(self, job: IngestionJob, total_results=10):
        file_paths = []
        workspace = self._prepare_workspace(job)
        for start in range(0, total_results, self.batch_size):
            url = self.BASE_URL.format(self.query, start, self.batch_size)
            response = requests.get(url)
            feed = feedparser.parse(response.content)
            for entry in feed.entries:
                file_paths.append(self.download_pdf(entry))

            time.sleep(3)  # To respect arXiv's rate limits
        return file_paths, workspace

    def download_pdf(self, entry):
        pdf_url = entry.id.replace('abs', 'pdf') + ".pdf"
        response = requests.get(pdf_url)
        if response.status_code == 200:
            file_path = os.path.join(self.download_dir, self.dataset_id, f"{entry.id.split('/')[-1]}.pdf")
            with open(file_path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded: {file_path}")
            return Path(file_path)
        else:
            print(f"Failed to download PDF for {entry.id}")

    def pdf_to_text(self, pdf_path):
        try:
            text_content = []
            
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                num_pages = len(pdf_reader.pages)
                
                for page_num in range(num_pages):
                    page = pdf_reader.pages[page_num]
                    text = page.extract_text()
                    text_content.append(f"\n{'='*60}\nPage {page_num + 1}/{num_pages}\n{'='*60}\n")
                    text_content.append(text)
            
            return ''.join(text_content)

        except Exception as e:
            print(f"  ✗ Conversion failed: {str(e)}")
            self.stats['failed_conversions'] += 1
            return None
        
    def pdf_url_to_text(pdf_url: str) -> str:
        """
        Download PDF from a URL directly into memory and extract its text.
        """
        # 1) Download PDF into memory
        response = requests.get(pdf_url)
        response.raise_for_status()

        pdf_bytes = io.BytesIO(response.content)

        # 2) Extract text using PyMuPDF
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            text = ""
            for page in doc:
                text += page.get_text()

        return text
            
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
        logger.info("Executing Arxiv ingestion job %s", job.job_id)
        files, workspace = self.fetch_papers(job)
        if not files:
            logger.warning("No files downloaded for job %s", job.job_id)
            return []
        uploaded = self.push_to_minio(job, files, workspace)
        logger.info("Completed job %s (%d objects uploaded)", job.job_id, len(uploaded))
        return uploaded