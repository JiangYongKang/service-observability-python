"""Request-boundary middleware: trace id, metrics, structured access logs.

Responsibilities, all decided at the single entry point:

1. Resolve the inbound ``X-Trace-Id``. A present-but-illegal value is rejected
   with 400 *before* the application runs (deterministic and distinguishable
   from an accepted request, which always echoes ``X-Trace-Source`` as
   ``upstream`` or ``generated``).
2. Bind the accepted id to the current context for the whole call chain.
3. Time the request and record one metric sample labelled by route template
   and coarse outcome (no concrete paths / ids in labels).
4. Emit one structured access log line carrying the trace id.
5. Convert any uncaught exception into a generic 500 body; the raw exception
   is logged server-side only.
"""
from __future__ import annotations

from time import perf_counter

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from obs.errors import DependencyError, classify_exception
from obs.logging_setup import get_logger
from obs.metrics import MetricLabels, MetricsCollector, Outcome
from obs.tracing import (
    TRACE_HEADER,
    TRACE_SOURCE_HEADER,
    TraceSource,
    bind_trace_id,
    resolve_trace_id,
)

INVALID_TRACE_BODY = (
    b'{"error":{"code":"invalid_trace_id",'
    b'"message":"trace id must be 32 lowercase hexadecimal characters"}}'
)
UNMATCHED_ROUTE = "unmatched"

logger = get_logger("obs.middleware")


def _outcome_for_status(status_code: int) -> Outcome:
    if status_code < 400:
        return Outcome.SUCCESS
    if status_code < 500:
        return Outcome.CLIENT_ERROR
    return Outcome.SERVER_ERROR


class ObservabilityMiddleware:
    """Pure ASGI middleware so it also wraps 404s and unhandled errors."""

    def __init__(self, app: ASGIApp, collector: MetricsCollector) -> None:
        self.app = app
        self.collector = collector

    async def _send_invalid_trace(
        self, scope: Scope, send: Send, decision_trace_id: str
    ) -> None:
        method = scope.get("method", "")
        start = perf_counter()
        # Log and count the rejection under a freshly generated id so the
        # event is traceable/measurable server-side; the client's illegal
        # value is never echoed nor written into any label.
        with bind_trace_id(decision_trace_id):
            logger.warning(
                "rejected request with invalid trace id",
                extra={"event": "invalid_trace_id", "method": method},
            )
            self.collector.record(
                MetricLabels(
                    method, self._route(scope), Outcome.CLIENT_ERROR
                ),
                perf_counter() - start,
            )
        await send(
            {
                "type": "http.response.start",
                "status": 400,
                "headers": [
                    (b"content-type", b"application/json"),
                    (
                        TRACE_SOURCE_HEADER.encode(),
                        TraceSource.INVALID.value.encode(),
                    ),
                ],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": INVALID_TRACE_BODY,
            }
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = {
            k.decode("latin-1").lower(): v.decode("latin-1")
            for k, v in scope.get("headers", [])
        }
        decision = resolve_trace_id(headers.get(TRACE_HEADER))
        if decision.source is TraceSource.INVALID:
            await self._send_invalid_trace(scope, send, decision.trace_id)
            return

        trace_id = decision.trace_id
        status_holder: dict[str, int] = {"status": 500}
        started = {"value": False}

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                started["value"] = True
                status_holder["status"] = message["status"]
                headers_out = message.setdefault("headers", [])
                headers_out.append((TRACE_HEADER.encode(), trace_id.encode()))
                headers_out.append(
                    (
                        TRACE_SOURCE_HEADER.encode(),
                        decision.source.value.encode(),
                    )
                )
            await send(message)

        method = scope.get("method", "")
        path = scope.get("path", "")
        start = perf_counter()
        with bind_trace_id(trace_id):
            try:
                await self.app(scope, receive, send_wrapper)
            except Exception as exc:  # noqa: BLE001 - boundary must not leak
                duration = perf_counter() - start
                category = (
                    exc.category
                    if isinstance(exc, DependencyError)
                    else classify_exception(exc)
                )
                logger.exception(
                    "unhandled error while processing request",
                    extra={
                        "event": "unhandled_exception",
                        "method": method,
                        "path": path,
                        "error_category": category.value,
                        "duration_ms": round(duration * 1000, 3),
                    },
                )
                self.collector.record(
                    MetricLabels(
                        method, self._route(scope), Outcome.SERVER_ERROR
                    ),
                    duration,
                )
                if started["value"]:
                    # Headers already sent: a replacement response is illegal
                    # in ASGI; re-raise so the server can abort the connection.
                    raise
                # Generic message only; no underlying exception detail.
                body = (
                    b'{"error":{"code":"internal_error",'
                    b'"message":"internal server error"}}'
                )
                await send(
                    {
                        "type": "http.response.start",
                        "status": 500,
                        "headers": [
                            (b"content-type", b"application/json"),
                            (TRACE_HEADER.encode(), trace_id.encode()),
                            (
                                TRACE_SOURCE_HEADER.encode(),
                                decision.source.value.encode(),
                            ),
                        ],
                    }
                )
                await send({"type": "http.response.body", "body": body})
                return

            duration = perf_counter() - start
            status_code = status_holder["status"]
            route = self._route(scope)
            self.collector.record(
                MetricLabels(method, route, _outcome_for_status(status_code)),
                duration,
            )
            logger.info(
                "request completed",
                extra={
                    "event": "request_completed",
                    "method": method,
                    "path": path,
                    "route": route,
                    "status_code": status_code,
                    "duration_ms": round(duration * 1000, 3),
                },
            )

    @staticmethod
    def _route(scope: Scope) -> str:
        route = scope.get("route")
        path_format = getattr(route, "path_format", None) if route else None
        return path_format or UNMATCHED_ROUTE
