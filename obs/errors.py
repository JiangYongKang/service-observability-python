"""Stable error categories for downstream dependency failures.

Classification is based on exception *types* and HTTP status codes, never on
exception message text, so categories stay stable across library versions and
locales. External responses only carry the stable category string; raw
exception details are kept server-side in structured logs.
"""
from __future__ import annotations

import json
from enum import Enum

import httpx

# Timeout types from the stdlib and from httpx/httpcore. Classification by
# isinstance is order-independent for these types and never reads messages.
_TIMEOUT_TYPES: tuple[type[BaseException], ...] = (
    TimeoutError,
    httpx.TimeoutException,
)
try:  # httpcore is an httpx dependency; keep import optional for safety.
    import httpcore

    _TIMEOUT_TYPES = (*_TIMEOUT_TYPES, httpcore.TimeoutException)
except ImportError:  # pragma: no cover - httpx always ships httpcore
    pass

_CONNECTION_TYPES: tuple[type[BaseException], ...] = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.NetworkError,
    httpx.PoolTimeout,
)
try:
    import httpcore

    _CONNECTION_TYPES = (
        *_CONNECTION_TYPES,
        httpcore.ConnectError,
        httpcore.ConnectTimeout,
        httpcore.NetworkError,
        httpcore.PoolTimeout,
    )
except ImportError:  # pragma: no cover
    pass

# Response bodies that cannot be parsed/validated.
_BAD_RESPONSE_TYPES: tuple[type[BaseException], ...] = (
    httpx.DecodingError,
    json.JSONDecodeError,
)


class DependencyErrorCategory(str, Enum):
    """Stable, mutually exclusive categories of downstream failures."""

    TIMEOUT = "timeout"
    CONNECTION_ERROR = "connection_error"
    UPSTREAM_5XX = "upstream_5xx"
    UPSTREAM_4XX = "upstream_4xx"
    BAD_RESPONSE = "bad_response"
    UNKNOWN = "unknown"


class DependencyError(Exception):
    """A downstream call failure carrying a stable category."""

    def __init__(self, category: DependencyErrorCategory) -> None:
        super().__init__(category.value)
        self.category = category


def classify_timeout(exc: BaseException) -> bool:
    """Return True when *exc* represents a timeout (type based).

    ``ConnectTimeout``/``PoolTimeout`` are reported as ``False`` here: while
    they technically are timeouts, semantically they describe a failure to
    obtain a connection and are reported under ``connection_error``.
    """
    if isinstance(exc, _CONNECTION_TYPES):
        return False
    return isinstance(exc, _TIMEOUT_TYPES)


def classify_exception(exc: BaseException) -> DependencyErrorCategory:
    """Map any exception raised by a downstream call to a stable category.

    The decision uses the exception type and, for HTTP status errors, the
    integer status code only -- never the message text.
    """
    # Already classified: keep the category as-is.
    if isinstance(exc, DependencyError):
        return exc.category

    if isinstance(exc, _CONNECTION_TYPES):
        return DependencyErrorCategory.CONNECTION_ERROR
    if classify_timeout(exc):
        return DependencyErrorCategory.TIMEOUT

    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status >= 500:
            return DependencyErrorCategory.UPSTREAM_5XX
        if status >= 400:
            return DependencyErrorCategory.UPSTREAM_4XX
        # Raised for a non-error status would be a client misuse: unknown.
        return DependencyErrorCategory.UNKNOWN

    if isinstance(exc, _BAD_RESPONSE_TYPES):
        return DependencyErrorCategory.BAD_RESPONSE

    return DependencyErrorCategory.UNKNOWN
