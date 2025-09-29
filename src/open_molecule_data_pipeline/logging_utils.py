"""Lightweight logging helpers providing a structlog-compatible interface."""

from __future__ import annotations

import logging
from typing import Any

try:  # pragma: no cover - optional dependency branch
    import structlog
except ImportError:  # pragma: no cover - fallback used in tests
    structlog = None  # type: ignore[assignment]


class _StructlogShim:
    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)

    def _log(self, level: int, event: str, **kwargs: Any) -> None:
        if kwargs:
            self._logger.log(level, "%s %s", event, kwargs)
        else:
            self._logger.log(level, event)

    def info(self, event: str, **kwargs: Any) -> None:
        self._log(logging.INFO, event, **kwargs)

    def warning(self, event: str, **kwargs: Any) -> None:
        self._log(logging.WARNING, event, **kwargs)

    def error(self, event: str, **kwargs: Any) -> None:
        self._log(logging.ERROR, event, **kwargs)

    def debug(self, event: str, **kwargs: Any) -> None:
        self._log(logging.DEBUG, event, **kwargs)

    def bind(self, **_: Any) -> "_StructlogShim":  # compatibility no-op
        return self


def get_logger(name: str):
    if structlog is not None:  # pragma: no cover - depends on dependency graph
        return structlog.get_logger(name)
    logging.basicConfig(level=logging.INFO)
    return _StructlogShim(name)


__all__ = ["get_logger"]
