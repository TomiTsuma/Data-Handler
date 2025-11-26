from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Sequence

from kaggle import KaggleApi

from core.exceptions.ingestion_error import KaggleDownloadError
from ingestion.kaggle.helpers import filter_downloaded_files, sanitize_dataset_name
from infrastructure.logging.logger import get_logger

logger = get_logger(__name__)


class KaggleClient:
    """Thin wrapper around the official Kaggle SDK."""

    def __init__(self) -> None:
        self.api = KaggleApi()
        self.api.authenticate()

    def download_dataset(
        self,
        owner_slug: str,
        dataset_slug: str,
        destination: Path,
        file_names: Sequence[str] | None = None,
        force: bool = False,
    ) -> List[Path]:
        dataset_ref = sanitize_dataset_name(owner_slug, dataset_slug)
        destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        try:
            logger.info("Downloading Kaggle dataset %s into %s", dataset_ref, destination)
            self.api.dataset_download_files(
                dataset_ref,
                path=str(destination),
                unzip=True,
                quiet=False,
                force=force,
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Failed to download Kaggle dataset %s: %s", dataset_ref, exc)
            raise KaggleDownloadError(str(exc)) from exc

        return filter_downloaded_files(destination, file_names)
