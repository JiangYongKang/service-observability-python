"""FastAPI application wiring observability middleware and example routes."""
from __future__ import annotations

import asyncio

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from obs.client import DownstreamClient
from obs.errors import DependencyError, DependencyErrorCategory
from obs.logging_setup import configure_logging, get_logger
from obs.metrics import MetricsCollector
from obs.middleware import ObservabilityMiddleware
from obs.tracing import current_trace_id

# Category -> HTTP status used on the service boundary.
_CATEGORY_STATUS: dict[DependencyErrorCategory, int] = {
    DependencyErrorCategory.TIMEOUT: 504,
    DependencyErrorCategory.CONNECTION_ERROR: 502,
    DependencyErrorCategory.UPSTREAM_5XX: 502,
    DependencyErrorCategory.UPSTREAM_4XX: 502,
    DependencyErrorCategory.BAD_RESPONSE: 502,
    DependencyErrorCategory.UNKNOWN: 500,
}


def create_app(
    client: DownstreamClient | None = None,
    collector: MetricsCollector | None = None,
) -> FastAPI:
    """Build the application.

    The downstream client and metrics collector can be injected by tests.
    """
    configure_logging()
    logger = get_logger("obs.app")
    app = FastAPI()
    metrics = collector or MetricsCollector()
    downstream = client or DownstreamClient()

    app.add_middleware(ObservabilityMiddleware, collector=metrics)

    @app.exception_handler(DependencyError)
    async def dependency_error_handler(
        request: Request, exc: DependencyError
    ) -> JSONResponse:
        # Stable, generic body per category; never the underlying error text.
        status_code = _CATEGORY_STATUS[exc.category]
        logger.error(
            "downstream dependency failure",
            extra={
                "event": "dependency_failure",
                "method": request.method,
                "path": request.url.path,
                "error_category": exc.category.value,
            },
        )
        return JSONResponse(
            status_code=status_code,
            content={
                "error": {
                    "code": "dependency_failure",
                    "category": exc.category.value,
                    "message": "downstream dependency request failed",
                }
            },
        )

    @app.get("/")
    async def read_root() -> dict[str, str]:
        # Unchanged legacy behaviour.
        return {"Hello": "World"}

    @app.get("/users/{user_id}")
    def read_user(user_id: str) -> dict[str, str | None]:
        # Synchronous endpoint: FastAPI runs it in a worker thread, and anyio
        # copies the request context into that thread. This therefore also
        # proves trace-id propagation across the sync/async boundary. The
        # high-cardinality path parameter is used in business logic but never
        # becomes a metric label; the route template /users/{user_id} does.
        logger.info(
            "handling read_user",
            extra={"event": "read_user", "user_id": user_id},
        )
        return {"user_id": user_id, "trace_id": current_trace_id()}

    @app.get("/quote")
    async def read_quote() -> dict[str, str | None]:
        quote = await downstream.fetch_quote()
        return {"quote": quote, "trace_id": current_trace_id()}

    @app.get("/async-check/{check_id}")
    async def async_check(check_id: str) -> dict[str, object]:
        """Exercise context stability across interleaved await points.

        Yields control many times; a context leak between concurrent requests
        would surface as a mismatched trace id. Pure observability test
        surface, no business meaning.
        """
        expected = current_trace_id()
        observed: list[str | None] = []
        for _ in range(20):
            await asyncio.sleep(0)
            observed.append(current_trace_id())
        return {
            "check_id": check_id,
            "trace_id": expected,
            "stable": all(value == expected for value in observed),
        }

    @app.get("/internal/metrics")
    async def internal_metrics() -> dict:
        return metrics.snapshot()

    # Exposed for tests that simulate raw-path traffic against templates.
    app.state.metrics = metrics
    app.state.downstream = downstream
    return app


app = create_app()
