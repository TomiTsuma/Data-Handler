from __future__ import annotations

import logging
import sys
from typing import Optional


def _configure_root_logger() -> None:
    if logging.getLogger().handlers:
        return
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=[handler])


def get_logger(name: Optional[str] = None) -> logging.Logger:
    _configure_root_logger()
    return logging.getLogger(name or "data-ingestion-service")
