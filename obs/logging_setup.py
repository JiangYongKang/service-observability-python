"""Structured JSON logging with trace-id injection.

A :class:`logging.Filter` enriches every record emitted while a trace id is
bound (see :mod:`obs.tracing`) with that id, so sync handlers, async handlers
and background tasks all log under the same id without threading it through
every call. Logging happens after context binding in the middleware, so every
request log line -- including exception lines -- carries the request id.
"""
from __future__ import annotations

import json
import logging
import sys
from typing import Any

from obs.tracing import current_trace_id

_LOGGER_NAME = "obs"
_RESERVED_ATTRS = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "taskName", "asctime", "message", "trace_id",
}


class JsonFormatter(logging.Formatter):
    """Render log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "trace_id": getattr(record, "trace_id", None),
        }
        # Structured extras passed via logger.info(..., extra={...}).
        for key, value in record.__dict__.items():
            if key not in _RESERVED_ATTRS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


class TraceContextFilter(logging.Filter):
    """Attach the current context's trace id to every record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = current_trace_id()
        return True


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure the application logger tree with JSON output.

    Idempotent: repeated calls reuse the existing handler.
    """
    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False
    if not any(
        getattr(h, "_obs_json_handler", False) for h in logger.handlers
    ):
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JsonFormatter())
        handler.addFilter(TraceContextFilter())
        handler._obs_json_handler = True  # type: ignore[attr-defined]
        logger.addHandler(handler)
    return logger


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a logger under the shared JSON-configured ``obs`` tree."""
    configure_logging()
    if name is None or name == _LOGGER_NAME:
        return logging.getLogger(_LOGGER_NAME)
    return logging.getLogger(f"{_LOGGER_NAME}.{name.removeprefix('obs.')}")
