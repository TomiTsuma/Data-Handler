from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Sequence


@dataclass(frozen=True)
class DataSource:
    """Base descriptor for any ingestion source."""

    name: str


@dataclass(frozen=True)
class KaggleDataSource(DataSource):
    """Description of a Kaggle dataset to ingest."""

    owner_slug: str
    dataset_slug: str
    file_names: Sequence[str] | None = field(default=None)

    def dataset_ref(self) -> str:
        return f"{self.owner_slug}/{self.dataset_slug}"

    def requires_filtering(self) -> bool:
        return bool(self.file_names)

    def files_to_pull(self) -> List[str] | None:
        if self.file_names is None:
            return None
        return [f.strip() for f in self.file_names if f.strip()]
    
@dataclass(frozen=True)
class ArxivDataSource(DataSource):
    """Description of an Arxiv dataset to ingest."""

    category: str
    dataset_slug: str
    file_names: Sequence[str] | None = field(default=None)

    def dataset_ref(self) -> str:
        return f"{self.dataset_slug}"

    def requires_filtering(self) -> bool:
        return bool(self.file_names)

    def files_to_pull(self) -> List[str] | None:
        if self.file_names is None:
            return None
        return [f.strip() for f in self.file_names if f.strip()]
