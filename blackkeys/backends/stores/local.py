from __future__ import annotations

_STORE: dict[str, object] = {}


def LocalCacheGet(key: str) -> object | None:
    return _STORE.get(key)


def LocalCacheSet(key: str, value: object) -> None:
    _STORE[key] = value


def LocalCacheClear() -> None:
    _STORE.clear()
