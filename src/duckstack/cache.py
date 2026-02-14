from __future__ import annotations

import time
from typing import Any

_cache: dict[str, tuple[float, Any]] = {}


def get(key: str) -> Any | None:
    if key in _cache:
        expires_at, value = _cache[key]
        if time.time() < expires_at:
            return value
        del _cache[key]
    return None


def put(key: str, value: Any, ttl_seconds: int) -> None:
    _cache[key] = (time.time() + ttl_seconds, value)


def invalidate(key: str) -> None:
    _cache.pop(key, None)


def clear() -> None:
    _cache.clear()
