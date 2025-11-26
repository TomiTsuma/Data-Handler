from __future__ import annotations

from dataclasses import dataclass


def ensure_set(value: str | None, message: str) -> str:
    if not value:
        raise ValueError(message)
    return value
