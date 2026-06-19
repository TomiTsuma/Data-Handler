from __future__ import annotations

from pathlib import Path
from typing import List

from core.exceptions.ingestion_error import IngestionError
from core.models.datasource import ArxivDataSource
from core.models.ingestion_job import IngestionJob
from core.utils.file_utils import clean_dir, ensure_dir
from infrastructure.logging.logger import get_logger
from infrastructure.minio.uploader import upload_file

import arxiv
import time
import os
import fitz  # PyMuPDF
import io
from minio import Minio
from minio.error import S3Error
import PyPDF2

logger = get_logger(__name__)


class arxiv_ResultToEntry:
    """Helper class to convert arxiv.Result to an entry-like object for compatibility."""

    def __init__(self, result: arxiv.Result):
        self._result = result
        self.title = result.title
        self.summary = result.summary
        self.id = result.entry_id
        self.arxiv_terms = result.categories

    def __getattr__(self, name):
        # Delegate any other attribute access to the underlying result
        return getattr(self._result, name)


class ArxivDownloader:
    CATEGORY = "cs.LG"
    MAX_RESULTS = 10          # arXiv allows up to 30k with multiple calls
    OUTPUT_DIR = "arxiv_papers"

    def __init__(self, arxiv_category, dataset_id, download_dir="data/tmp", batch_size=1000, query_mode="category", max_results=100, keywords=None):
        self.query = arxiv_category
        self.download_dir = download_dir
        self.batch_size = batch_size
        self.dataset_id = dataset_id
        self.query_mode = query_mode  # "category" or "query"
        self.max_results = max_results  # Maximum number of papers to download
        self.keywords = keywords or []  # List of keywords to filter papers
        self.client = arxiv.Client()
        os.makedirs(download_dir, exist_ok=True)

    def _build_query_string(self):
        """Build the arXiv API query string based on keywords."""
        if self.query_mode == "category":
            # Category-based search
            return f"cat:{self.query}"
        else:
            # Keyword-based search with AND logic
            if self.keywords:
                # Convert keywords to arXiv query format
                # Each keyword becomes "all:(keyword)" and they're joined with AND
                keyword_parts = [f"all:({kw})" for kw in self.keywords]
                query = "+AND+".join(keyword_parts)
                return query
            else:
                # Fallback to category search
                return f"cat:{self.query}"

    def _prepare_workspace(self, job: IngestionJob) -> Path:
        workspace = job.workspace_path() / job.job_id
        ensure_dir(workspace)
        clean_dir(workspace)
        return workspace

    def _assert_kaggle_source(self, job: IngestionJob) -> ArxivDataSource:
        if not isinstance(job.source, ArxivDataSource):
            raise IngestionError("ArxivDownloader requires a ArxivDataSource")
        return job.source

    def _matches_keywords(self, entry) -> bool:
        """Check if a paper matches ALL of the specified keywords."""
        if not self.keywords:
            return True

        # Combine title, abstract, and terms for keyword matching
        title = getattr(entry, 'title', '').lower()
        abstract = getattr(entry, 'summary', '').lower()

        # Handle arxiv library Result objects - terms are strings, not objects
        arxiv_terms = getattr(entry, 'arxiv_terms', [])
        if arxiv_terms:
            # Check if terms are strings or objects with .term attribute
            if isinstance(arxiv_terms[0], str):
                terms_str = ' '.join(arxiv_terms).lower()
            else:
                terms_str = ' '.join([t.term for t in arxiv_terms]).lower()
        else:
            terms_str = ''

        combined_text = f"{title} {abstract} {terms_str}"

        # Check if ANY keyword matches (OR logic)
        for keyword in self.keywords:
            if keyword.lower() in combined_text:
                return True
        return False

    def fetch_papers(self, job: IngestionJob):
        file_paths = []
        workspace = self._prepare_workspace(job)
        self.workspace = workspace  # Store workspace for download_pdf

        search = arxiv.Search(
            query=self._build_query_string(),
            max_results=self.max_results,
            sort_by=arxiv.SortCriterion.SubmittedDate
        )

        for result in self.client.results(search):
            # Stop if we've reached max_results
            if len(file_paths) >= self.max_results:
                break

            # Convert arxiv.Result to a compatible entry-like object
            entry = arxiv_ResultToEntry(result)

            # Filter by keywords if specified
            if self._matches_keywords(entry):
                file_paths.append(self.download_pdf(result))

            time.sleep(1)  # To respect arXiv's rate limits
        return file_paths

    def download_pdf(self, result: arxiv.Result) -> Path:
        """Download PDF using arxiv library's built-in download method."""
        pdf_filename = f"{result.entry_id.split('/')[-1]}.pdf"
        pdf_path = self.workspace / pdf_filename

        # Use arxiv library's download method which handles retries and errors
        # New API: dirpath and filename as positional arguments
        try:
            result.download_pdf(dirpath=str(self.workspace), filename=pdf_filename)
        except Exception as e:
            print(f"Download failed for {result.entry_id}: {e}")
            return Path()

        if pdf_path.exists():
            print(f"Downloaded: {pdf_path}")
            return pdf_path
        else:
            print(f"Failed to download PDF for {result.entry_id}")
            return Path()

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
        # 1) Download PDF into memory using arxiv library
        paper_id = pdf_url.split('/')[-1].replace('.pdf', '')
        result = arxiv.Paper.from_id(paper_id)
        pdf_bytes = io.BytesIO(result.download_pdf())

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
        # Update from source if available
        if isinstance(job.source, ArxivDataSource):
            self.query = job.source.category
            self.dataset_id = job.source.dataset_slug
            self.query_mode = job.source.query_mode
            self.keywords = list(job.source.keywords) if job.source.keywords else []
        files = self.fetch_papers(job)
        if not files:
            logger.warning("No files downloaded for job %s", job.job_id)
            return []
        uploaded = self.push_to_minio(job, files, self.workspace)
        logger.info("Completed job %s (%d objects uploaded)", job.job_id, len(uploaded))
        return uploaded