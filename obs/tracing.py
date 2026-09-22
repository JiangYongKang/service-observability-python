"""Trace identifier resolution and context propagation.

The trace id is decided once at the request boundary and then carried by a
:class:`contextvars.ContextVar`. asyncio gives each request task its own
context copy, and anyio copies that context into worker threads for sync
endpoints, so the id is:

* the same across every step (async or sync) of one request;
* isolated between concurrent requests -- ids never bleed across tasks.
"""
from __future__ import annotations

import contextvars
import re
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum

TRACE_HEADER = "x-trace-id"
TRACE_SOURCE_HEADER = "x-trace-source"

# Canonical format: 32 lowercase hexadecimal characters (UUID without dashes).
TRACE_ID_PATTERN = r"^[0-9a-f]{32}$"
_TRACE_ID_RE = re.compile(TRACE_ID_PATTERN)

_trace_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "trace_id", default=None
)


class TraceSource(str, Enum):
    """Where the trace id used for a request came from."""

    UPSTREAM = "upstream"
    GENERATED = "generated"
    INVALID = "invalid"


@dataclass(frozen=True, slots=True)
class TraceDecision:
    """Result of resolving an inbound trace id."""

    trace_id: str
    source: TraceSource


def is_valid_trace_id(value: str) -> bool:
    """Return True when *value* is a syntactically legal trace id."""
    return bool(_TRACE_ID_RE.fullmatch(value))


def _generate() -> str:
    return uuid.uuid4().hex


def resolve_trace_id(inbound: str | None) -> TraceDecision:
    """Decide the trace id for a request from its inbound header value.

    * header absent (``None``) -> a new random id, source ``generated``
    * well-formed value        -> echoed verbatim, source ``upstream``
    * present but empty/illegal (including whitespace-only) -> a new random
      id for internal logging and source ``invalid``; the middleware rejects
      the request with 400 before it reaches the application, so the generated
      id never services a request.
    """
    if inbound is None:
        return TraceDecision(_generate(), TraceSource.GENERATED)
    value = inbound.strip()
    if value and is_valid_trace_id(value):
        return TraceDecision(value, TraceSource.UPSTREAM)
    return TraceDecision(_generate(), TraceSource.INVALID)


def current_trace_id() -> str | None:
    """Return the trace id bound to the current context, if any."""
    return _trace_id_var.get()


@contextmanager
def bind_trace_id(trace_id: str) -> Iterator[None]:
    """Bind *trace_id* to the current context for the duration of a block."""
    token = _trace_id_var.set(trace_id)
    try:
        yield
    finally:
        _trace_id_var.reset(token)
