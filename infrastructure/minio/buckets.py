from __future__ import annotations

from minio.error import S3Error

from core.exceptions.storage_error import BucketCreationError
from infrastructure.logging.logger import get_logger
from infrastructure.minio.client import get_minio_client

logger = get_logger(__name__)


def ensure_bucket(bucket_name: str) -> None:
    client = get_minio_client()
    try:
        if client.bucket_exists(bucket_name):
            return
        client.make_bucket(bucket_name)
        logger.info("Created MinIO bucket %s", bucket_name)
    except S3Error as exc:
        logger.exception("Could not ensure bucket %s: %s", bucket_name, exc)
        raise BucketCreationError(str(exc)) from exc
