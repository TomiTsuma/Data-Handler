from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

from dotenv import load_dotenv
from minio import Minio

load_dotenv()


@dataclass(frozen=True)
class MinioSettings:
    endpoint: str
    access_key: str
    secret_key: str
    region: str | None = None
    secure: bool = False


def _env_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


@lru_cache()
def get_minio_settings() -> MinioSettings:
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    region = os.getenv("MINIO_REGION")
    secure = _env_bool(os.getenv("MINIO_USE_SSL"), default=False)
    if not endpoint or not access_key or not secret_key:
        raise RuntimeError("MINIO_ENDPOINT, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY must be set")
    return MinioSettings(endpoint, access_key, secret_key, region, secure)


@lru_cache()
def get_minio_client() -> Minio:
    settings = get_minio_settings()
    return Minio(
        settings.endpoint,
        access_key=settings.access_key,
        secret_key=settings.secret_key,
        region=settings.region,
        secure=settings.secure,
    )
