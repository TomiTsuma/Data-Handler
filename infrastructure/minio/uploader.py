from __future__ import annotations

from pathlib import Path

from minio.error import S3Error

from core.exceptions.storage_error import ObjectUploadError
from infrastructure.logging.logger import get_logger
from infrastructure.minio import buckets
from infrastructure.minio.client import get_minio_client

logger = get_logger(__name__)


def upload_file(bucket: str, source_path: Path, object_name: str, content_type: str | None = None) -> None:
    client = get_minio_client()
    buckets.ensure_bucket(bucket)
    try:
        client.fput_object(bucket, object_name, str(source_path), content_type=content_type)
        logger.info("Uploaded %s to bucket=%s as %s", source_path, bucket, object_name)
    except S3Error as exc:
        logger.exception("Failed to upload %s to MinIO: %s", source_path, exc)
        raise ObjectUploadError(str(exc)) from exc


def upload_from_memory(bucket: str, object_name, text_stream, text_bytes, content_type) -> None:
    client = get_minio_client()
    buckets.ensure_bucket(bucket)
    try:
        client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=text_stream,
            length=len(text_bytes),
            content_type=content_type
        )
        logger.info("Uploaded to bucket=%s as %s", bucket, object_name)
    except S3Error as exc:
        logger.exception("Failed to upload to MinIO: %s", exc)
        raise ObjectUploadError(str(exc)) from exc
