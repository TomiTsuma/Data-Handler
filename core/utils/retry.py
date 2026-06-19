from __future__ import annotations

import functools
import time
from typing import Any, Callable, Type


def retry(
    exceptions: tuple[Type[Exception], ...],
    attempts: int = 3,
    backoff_seconds: float = 1.0,
) -> Callable:
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any):
            last_error: Exception | None = None
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_error = exc
                    if attempt == attempts:
                        raise
                    time.sleep(backoff_seconds * attempt)
            if last_error:
                raise last_error

        return wrapper

    return decorator
