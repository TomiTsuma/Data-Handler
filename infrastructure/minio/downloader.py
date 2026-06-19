from __future__ import annotations

from pathlib import Path

from minio.error import S3Error

from core.exceptions.storage_error import StorageError
from infrastructure.logging.logger import get_logger
from infrastructure.minio.client import get_minio_client

logger = get_logger(__name__)


def download_file(bucket: str, object_name: str, destination: Path) -> Path:
    client = get_minio_client()
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        client.fget_object(bucket, object_name, str(destination))
        logger.info("Downloaded %s/%s to %s", bucket, object_name, destination)
    except S3Error as exc:
        logger.exception("Failed to download %s/%s: %s", bucket, object_name, exc)
        raise StorageError(str(exc)) from exc
    return destination
