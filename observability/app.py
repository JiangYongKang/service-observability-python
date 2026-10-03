"""FastAPI 应用装配：保留既有根路由，新增下游代理、指标读取与错误处理。

行为约定：

* ``GET /`` 的正常响应保持 ``{"Hello": "World"}`` 不变；
* ``GET /items/{item_id}`` 经 :class:`DownstreamClient` 调用下游并透传
  追踪标识，成功返回下游 JSON；
* ``GET /aggregate`` 并发调用多个下游依赖并合并结果：逐依赖归因、
  总时长受控、整体结论确定（ok/partial/failed）；
* 下游异常一律由统一句柄映射为固定结构错误体，不泄露底层细节；
* ``GET /metrics`` 导出进程内指标快照（无高基数信息）。
"""

from __future__ import annotations

import os
from typing import Any

from contextlib import asynccontextmanager

from dataclasses import asdict

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from observability.aggregate import (
    DEFAULT_TOTAL_TIMEOUT_SECONDS,
    AggregateStatus,
    DependencySpec,
    run_aggregate,
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

#: 聚合查询的依赖白名单：名称（可进指标标签）-> 下游路径模板。
#: 标签基数有界的关键：依赖名只能来自这里的固定配置，不取自请求输入。
AGGREGATE_DEPENDENCY_PATHS: dict[str, str] = {
    "items": "/deps/items",
    "inventory": "/deps/inventory",
    "pricing": "/deps/pricing",
}

#: 整次聚合的总时长上限（秒），超时后未完成依赖按 downstream_timeout 收口
AGGREGATE_TOTAL_TIMEOUT = float(
    os.environ.get("AGGREGATE_TOTAL_TIMEOUT", str(DEFAULT_TOTAL_TIMEOUT_SECONDS))
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
    async def aggregate(
        request: Request,
        deps: str | None = None,
        downstream: DownstreamClient = Depends(_get_downstream),
    ) -> JSONResponse:
        specs = _resolve_dependency_specs(deps)
        total_timeout = getattr(
            request.app.state, "aggregate_timeout", AGGREGATE_TOTAL_TIMEOUT
        )
        report = await run_aggregate(specs, downstream, total_timeout=total_timeout)
        dependencies = [
            {
                "name": o.name,
                "status": "ok" if o.ok else "failed",
                "category": o.category,
                "data": o.data,
            }
            for o in report.outcomes
        ]
        body: dict[str, Any] = {
            "status": report.status.value,
            "dependencies": dependencies,
            "trace_id": report.trace_id,
        }
        if report.status is not AggregateStatus.FAILED:
            # 全部成功或部分失败：200，调用方按 status 字段与逐依赖条目区分
            return JSONResponse(status_code=200, content=body)
        # 全部失败：整体归因到固定类别，复用既有类别->状态码/文案映射，
        # 内部类别头供中间件把本次请求记入对应指标结果维度（出口被剥离）
        category = ErrorCategory(report.overall_category or ErrorCategory.INTERNAL_ERROR)
        body["error"] = CATEGORY_MESSAGE[category]
        body["category"] = category.value
        return JSONResponse(
            status_code=CATEGORY_STATUS[category],
            content=body,
            headers={ERROR_CATEGORY_HEADER: category.value},
        )


def _resolve_dependency_specs(deps: str | None) -> list[DependencySpec]:
    """把 ``deps`` 查询参数解析为依赖调用清单。

    缺省或为空 → 全部已配置依赖；显式给出时只允许白名单内的名称，
    未知名称一律 400（固定文案，不回显输入）。返回按名称排序的清单，
    与请求参数顺序无关，保证行为确定。
    """
    if deps is None or deps.strip() == "":
        names = sorted(AGGREGATE_DEPENDENCY_PATHS)
    else:
        names = sorted({part.strip() for part in deps.split(",") if part.strip()})
        unknown = [name for name in names if name not in AGGREGATE_DEPENDENCY_PATHS]
        if not names or unknown:
            raise HTTPException(status_code=400, detail="unknown dependency name")
    return [DependencySpec(name, AGGREGATE_DEPENDENCY_PATHS[name]) for name in names]


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
