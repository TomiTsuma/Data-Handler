from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Optional

from core.models.datasource import DataSource


@dataclass(frozen=True)
class Destination:
    bucket: str
    prefix: str = ""

    def object_name(self, relative_path: Path) -> str:
        prefix = self.prefix.strip("/")
        parts = [part for part in (prefix, relative_path.as_posix()) if part]
        return "/".join(parts)


@dataclass
class IngestionJob:
    """Description of an ingestion job to be executed by a pipeline."""

    job_id: str
    source: DataSource
    destination: Destination
    workspace: Path = field(default_factory=lambda: Path("data/tmp"))
    kind: Literal["kaggle"] = "kaggle"

    def workspace_path(self) -> Path:
        return Path(self.workspace).expanduser().resolve()
