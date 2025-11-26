from __future__ import annotations

from pathlib import Path
from typing import List, Sequence

from core.utils.file_utils import list_files


def sanitize_dataset_name(owner_slug: str, dataset_slug: str) -> str:
    return f"{owner_slug.strip()}/{dataset_slug.strip()}"


def filter_downloaded_files(directory: Path, file_names: Sequence[str] | None) -> List[Path]:
    files = list_files(directory)
    if not file_names:
        return files
    desired = {name.strip() for name in file_names if name.strip()}
    return [path for path in files if path.name in desired]
