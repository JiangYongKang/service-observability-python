"""FastAPI 应用装配：保留既有根路由，新增下游代理、指标读取与错误处理。

行为约定：

* ``GET /`` 的正常响应保持 ``{"Hello": "World"}`` 不变；
* ``GET /items/{item_id}`` 经 :class:`DownstreamClient` 调用下游并透传
  追踪标识，成功返回下游 JSON；
* 下游异常一律由统一句柄映射为固定结构错误体，不泄露底层细节；
* ``GET /metrics`` 导出进程内指标快照（无高基数信息）。
"""

from __future__ import annotations

import os
from typing import Any

from contextlib import asynccontextmanager

from dataclasses import asdict

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from observability.aggregation import (
    DEFAULT_TOTAL_TIMEOUT_SECONDS,
    AggregateResult,
    AggregateVerdict,
    AggregationEngine,
    DependencySpec,
    DependencyStatus,
)
from observability.context import current_trace_id
from observability.downstream import DownstreamClient
from observability.errors import (
    CATEGORY_MESSAGE,
    CATEGORY_STATUS,
    ERROR_CATEGORY_HEADER,
    DownstreamError,
    ErrorBody,
    ErrorCategory,
)
from observability.logging_setup import configure_logging, get_logger
from observability.middleware import ObservabilityMiddleware
from observability.metrics import registry

configure_logging()
logger = get_logger("observability.app")

DOWNSTREAM_BASE_URL = os.environ.get("DOWNSTREAM_BASE_URL", "http://downstream.local")
DOWNSTREAM_TIMEOUT = float(os.environ.get("DOWNSTREAM_TIMEOUT", "2.0"))
AGGREGATE_TOTAL_TIMEOUT = float(
    os.environ.get("AGGREGATE_TOTAL_TIMEOUT", str(DEFAULT_TOTAL_TIMEOUT_SECONDS))
)

#: 聚合默认扇出的依赖集合（名称稳定、进指标标签；可被 app.state 覆盖）
DEFAULT_AGGREGATE_DEPENDENCIES: tuple[DependencySpec, ...] = (
    DependencySpec(name="items", path="/items/aggregate"),
)


def create_app() -> FastAPI:
    """构造应用实例（工厂模式，便于测试注入下游客户端）。"""

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        try:
            yield
        finally:
            client = getattr(application.state, "downstream_client", None)
            if client is not None:
                await client.aclose()

    app = FastAPI(title="service-observability-python", lifespan=lifespan)
    app.add_middleware(ObservabilityMiddleware)
    _register_routes(app)
    _register_exception_handlers(app)
    return app


def _build_default_client() -> DownstreamClient:
    return DownstreamClient(DOWNSTREAM_BASE_URL, timeout=DOWNSTREAM_TIMEOUT)


def _get_downstream(request: Request) -> DownstreamClient:
    """依赖：允许测试通过 ``app.state.downstream_client`` 注入替身。"""
    client = getattr(request.app.state, "downstream_client", None)
    if client is None:
        client = _build_default_client()
        request.app.state.downstream_client = client
    return client


def _get_aggregate_dependencies(request: Request) -> list[DependencySpec]:
    """依赖：允许测试通过 ``app.state.aggregate_dependencies`` 注入依赖集合。"""
    deps = getattr(request.app.state, "aggregate_dependencies", None)
    if deps is None:
        deps = list(DEFAULT_AGGREGATE_DEPENDENCIES)
    return deps


def _get_aggregate_timeout(request: Request) -> float:
    """依赖：允许测试通过 ``app.state.aggregate_total_timeout`` 覆盖总时长上限。"""
    return float(getattr(request.app.state, "aggregate_total_timeout", AGGREGATE_TOTAL_TIMEOUT))


#: 聚合整体结论 → 对外 HTTP 状态码（部分失败仍返回数据，故为 200）
_VERDICT_STATUS: dict[AggregateVerdict, int] = {
    AggregateVerdict.OK: 200,
    AggregateVerdict.PARTIAL: 200,
    AggregateVerdict.FAILED: 502,
}

#: 聚合整体结论 → 内部错误类别头（中间件据此归类指标结果维度，出口剥离）
_VERDICT_CATEGORY_HEADER: dict[AggregateVerdict, str] = {
    AggregateVerdict.PARTIAL: "partial_failure",
    AggregateVerdict.FAILED: "aggregate_failed",
}


def _render_aggregate_response(result: AggregateResult) -> JSONResponse:
    """把聚合收口结果渲染为对外响应。

    响应体只含稳定结论、逐依赖状态/类别与追踪标识；上游状态码、地址、
    堆栈等底层细节只进服务端日志，不进响应。
    """
    trace_id = current_trace_id() or "-"
    dependencies: dict[str, Any] = {}
    for outcome in result.outcomes:
        if outcome.status is DependencyStatus.OK:
            dependencies[outcome.name] = {"status": "ok", "data": outcome.data}
        else:
            category = outcome.category or ErrorCategory.INTERNAL_ERROR
            dependencies[outcome.name] = {
                "status": "error",
                "category": category.value,
            }
    logger.info(
        "aggregate request completed",
        extra={
            "component": "app",
            "verdict": result.verdict.value,
            "dependencies": {
                name: entry["status"] for name, entry in dependencies.items()
            },
        },
    )
    body = {
        "status": result.verdict.value,
        "trace_id": trace_id,
        "dependencies": dependencies,
    }
    headers: dict[str, str] = {}
    category = _VERDICT_CATEGORY_HEADER.get(result.verdict)
    if category is not None:
        headers[ERROR_CATEGORY_HEADER] = category
    return JSONResponse(
        status_code=_VERDICT_STATUS[result.verdict],
        content=body,
        headers=headers,
    )


def _register_routes(app: FastAPI) -> None:
    @app.get("/", include_in_schema=False)
    async def read_root() -> dict[str, str]:
        # 既有接口的正常响应行为保持不变
        return {"Hello": "World"}

    @app.get("/items/{item_id}")
    async def get_item(
        item_id: str,
        request: Request,
        downstream: DownstreamClient = Depends(_get_downstream),
    ) -> dict[str, Any]:
        # 追踪标识由中间件绑定在上下文，客户端自动透传；异常交由句柄归因
        path = f"/items/{item_id}"
        if request.url.query:
            path = f"{path}?{request.url.query}"
        return await downstream.get_json(path)

    @app.get("/metrics", include_in_schema=False)
    async def get_metrics() -> dict[str, Any]:
        return registry.snapshot()

    @app.get("/aggregate")
    async def get_aggregate(
        request: Request,
        downstream: DownstreamClient = Depends(_get_downstream),
        dependencies: list[DependencySpec] = Depends(_get_aggregate_dependencies),
        total_timeout: float = Depends(_get_aggregate_timeout),
    ) -> JSONResponse:
        # 骨架：扇出/收口逻辑在 AggregationEngine 中实现，此处仅装配
        engine = AggregationEngine(
            downstream, dependencies, total_timeout=total_timeout
        )
        result = await engine.aggregate()
        return _render_aggregate_response(result)


def _register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(DownstreamError)
    async def _handle_downstream(request: Request, exc: DownstreamError) -> JSONResponse:
        category: ErrorCategory = exc._category
        trace_id = current_trace_id() or "-"
        # 底层细节只进日志（含上游状态码这一脱敏数值），不进响应
        logger.warning(
            "downstream call failed",
            extra={
                "component": "app",
                "path": request.url.path,
                "category": category.value,
                "upstream_status": exc.upstream_status,
                "trace_id": trace_id,
            },
            exc_info=True,
        )
        body = ErrorBody(
            error=CATEGORY_MESSAGE[category],
            category=category.value,
            trace_id=trace_id,
        )
        return JSONResponse(
            status_code=CATEGORY_STATUS[category],
            content=asdict(body),
            headers={ERROR_CATEGORY_HEADER: category.value},
        )

    @app.exception_handler(Exception)
    async def _handle_unexpected(request: Request, exc: Exception) -> JSONResponse:
        trace_id = current_trace_id() or "-"
        logger.error(
            "unhandled exception",
            extra={"component": "app", "path": request.url.path, "trace_id": trace_id},
            exc_info=True,
        )
        body = ErrorBody(
            error=CATEGORY_MESSAGE[ErrorCategory.INTERNAL_ERROR],
            category=ErrorCategory.INTERNAL_ERROR.value,
            trace_id=trace_id,
        )
        return JSONResponse(
            status_code=CATEGORY_STATUS[ErrorCategory.INTERNAL_ERROR],
            content=asdict(body),
            headers={ERROR_CATEGORY_HEADER: ErrorCategory.INTERNAL_ERROR.value},
        )


app = create_app()
