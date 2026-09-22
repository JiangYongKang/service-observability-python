"""Downstream dependency client.

Wraps httpx so every failure -- timeout, connection failure, non-success
status, or malformed response body -- is mapped to a
:class:`~obs.errors.DependencyError` carrying a stable
:class:`~obs.errors.DependencyErrorCategory`. The mapping uses exception
types/status codes only; raw exception text never crosses the service
boundary.

A custom httpx transport can be injected for deterministic tests.
"""
from __future__ import annotations

from typing import Any

import httpx

from obs.errors import DependencyError, DependencyErrorCategory, classify_exception
from obs.tracing import TRACE_HEADER, current_trace_id

DEFAULT_QUOTE_URL = "https://downstream.example/api/quote"


class DownstreamClient:
    """Small async client for an example downstream dependency."""

    def __init__(
        self,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 1.0,
        url: str = DEFAULT_QUOTE_URL,
    ) -> None:
        self._transport = transport
        self._timeout = timeout
        self._url = url

    async def fetch_quote(self) -> str:
        """Fetch and validate the quote; raise :class:`DependencyError`.

        Success contract: HTTP 2xx with a JSON object containing a non-empty
        string field ``quote``. Anything else raises with a stable category.
        """
        try:
            # Propagate the request-bound trace id downstream so the whole
            # call chain can be correlated.
            headers: dict[str, str] = {}
            trace_id = current_trace_id()
            if trace_id is not None:
                headers[TRACE_HEADER] = trace_id
            async with httpx.AsyncClient(
                transport=self._transport, timeout=self._timeout
            ) as http:
                response = await http.get(self._url, headers=headers)
                response.raise_for_status()
                data: Any = response.json()
                if not isinstance(data, dict):
                    raise DependencyError(DependencyErrorCategory.BAD_RESPONSE)
                quote = data.get("quote")
                if not isinstance(quote, str) or not quote.strip():
                    raise DependencyError(DependencyErrorCategory.BAD_RESPONSE)
                return quote
        except DependencyError:
            raise
        except Exception as exc:  # noqa: BLE001 - mapped to stable categories
            raise DependencyError(classify_exception(exc)) from None
