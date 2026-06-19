#!/usr/bin/env python3
"""Upload all PDF files from data/tmp to Minio research-papers bucket."""

from pathlib import Path
from minio.error import S3Error

from core.exceptions.storage_error import ObjectUploadError
from infrastructure.logging.logger import get_logger
from infrastructure.minio import buckets
from infrastructure.minio.client import get_minio_client

logger = get_logger(__name__)


def upload_pdfs_to_minio(pdf_dir: Path, bucket: str = "research-papers") -> None:
    """Upload all PDF files from a directory to Minio.

    Args:
        pdf_dir: Path to directory containing PDF files.
        bucket: Target Minio bucket name.
    """
    client = get_minio_client()
    buckets.ensure_bucket(bucket)

    pdf_files = list(pdf_dir.glob("**/*.pdf"))
    if not pdf_files:
        logger.warning("No PDF files found in %s", pdf_dir)
        return

    logger.info("Found %d PDF files in %s", len(pdf_files), pdf_dir)

    for pdf_path in pdf_files:
        # Use relative path as object name to preserve directory structure
        object_name = pdf_path.relative_to(pdf_dir)
        try:
            client.fput_object(bucket, str(object_name), str(pdf_path))
            logger.info("Uploaded %s -> %s/%s", pdf_path, bucket, object_name)
        except S3Error as exc:
            logger.exception("Failed to upload %s to Minio: %s", pdf_path, exc)
            raise ObjectUploadError(str(exc)) from exc


if __name__ == "__main__":
    upload_pdfs_to_minio(Path("data/tmp"))
