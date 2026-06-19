from __future__ import annotations

import shutil
from pathlib import Path
from typing import Iterable, List


def ensure_dir(path: Path) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def clean_dir(path: Path) -> None:
    path = Path(path)
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        return
    for child in path.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def list_files(path: Path) -> List[Path]:
    path = Path(path)
    if not path.exists():
        return []
    return [child for child in path.rglob("*") if child.is_file()]
