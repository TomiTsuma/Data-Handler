from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from core.models.ingestion_job import IngestionJob


class BasePipeline(ABC):
    """Abstract pipeline that executes an ingestion job."""

    @abstractmethod
    def can_handle(self, job: IngestionJob) -> bool:
        ...

    @abstractmethod
    def run(self, job: IngestionJob) -> Any:
        ...
