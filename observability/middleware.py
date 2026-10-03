"""ASGI 中间件：入口确定追踪标识、绑定上下文、记录分维度指标。

为什么用纯 ASGI 而非 ``BaseHTTPMiddleware``：中间件与路由在同一任务
上下文内 ``await`` 下游应用，``contextvars`` 的绑定对端点及其发起的
下游调用天然可见，且每个请求任务持有独立的 Context 拷贝，不会串号。

职责：

1. 入口从 ``X-Request-ID`` 解析/生成标识，写回响应头
   ``X-Request-ID`` 与 ``X-Trace-Source``（upstream/generated/rejected）；
2. 在整个请求处理期间把标识绑定到 contextvars；
3. 包装响应：路由模板（非原始路径）作为 route 标签；依据状态码与
   ``X-Error-Category`` 头把结果归入封闭的 :class:`Result`；标识来源
   作为取值固定的 ``trace`` 标签；
4. 记录计数与延迟；非法标识不影响请求处理，只在响应头与指标上可区分；
5. 兜底收尾：下游应用抛出未预料异常且响应尚未开始时，在上下文仍绑定
   标识的情况下记录日志并发出统一 500 错误响应，保证响应头、结构化
   日志与指标结果维度上的追踪标识一致。
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict

from starlette.datastructures import Headers
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from observability.context import TRACE_HEADER, bind_trace_id, resolve_trace_id
from observability.errors import (
    CATEGORY_MESSAGE,
    CATEGORY_STATUS,
    ERROR_CATEGORY_HEADER,
    ErrorBody,
    ErrorCategory,
)
from observability.logging_setup import get_logger
from observability.metrics import (
    UNMATCHED_ROUTE,
    LatencySample,
    MetricsRegistry,
    Result,
    registry as default_registry,
)

logger = get_logger("observability.middleware")

#: 响应头：回传最终标识，便于调用方与日志/指标对齐
RESPONSE_TRACE_HEADER = TRACE_HEADER
#: 响应头：标识最终标识来源（upstream/generated/rejected）
TRACE_SOURCE_HEADER = "X-Trace-Source"

_CATEGORY_TO_RESULT = {
    b"downstream_timeout": Result.DOWNSTREAM_TIMEOUT,
    b"downstream_connection": Result.DOWNSTREAM_CONNECTION,
    b"downstream_status": Result.DOWNSTREAM_STATUS,
    b"downstream_invalid_response": Result.DOWNSTREAM_INVALID,
    b"internal_error": Result.INTERNAL_ERROR,
    b"partial_failure": Result.PARTIAL_FAILURE,
    b"aggregate_failed": Result.AGGREGATE_FAILED,
}


class ObservabilityMiddleware:
    """纯 ASGI 中间件：与下游应用在同一任务上下文内，保证 contextvars 传播。"""

    def __init__(
        self,
        app: ASGIApp,
        metrics_registry: MetricsRegistry | None = None,
    ) -> None:
        self.app = app
        self.registry = metrics_registry or default_registry

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        await self._handle_http(scope, receive, send)

    async def _handle_http(self, scope: Scope, receive: Receive, send: Send) -> None:
        request_headers = Headers(scope=scope)
        identity = resolve_trace_id(request_headers.get(TRACE_HEADER.lower()))
        token = bind_trace_id(identity.trace_id)

        status_code = 500
        category_header: bytes | None = None
        response_started = False

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code, category_header, response_started
            if message["type"] == "http.response.start":
                response_started = True
                status_code = message["status"]
                cleaned: list[tuple[bytes, bytes]] = []
                for name, value in message.get("headers", []):
                    if name.lower() == ERROR_CATEGORY_HEADER.lower().encode():
                        category_header = value
                        continue  # 内部类别头不对外透出
                    cleaned.append((name, value))
                cleaned.append((RESPONSE_TRACE_HEADER.encode(), identity.trace_id.encode()))
                cleaned.append((TRACE_SOURCE_HEADER.encode(), identity.source.value.encode()))
                message["headers"] = cleaned
            await send(message)

        start = time.perf_counter()
        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            if response_started:
                # 响应已开始，无法改写为错误响应，交由外层处理
                raise
            # 兜底收尾：在上下文仍绑定时记录日志并发出统一错误响应，
            # 保证响应头 / 日志 / 指标上的追踪标识一致
            status_code = CATEGORY_STATUS[ErrorCategory.INTERNAL_ERROR]
            category_header = ErrorCategory.INTERNAL_ERROR.value.encode()
            logger.error(
                "unhandled exception",
                extra={"component": "middleware", "path": scope.get("path", "-")},
                exc_info=True,
            )
            body = ErrorBody(
                error=CATEGORY_MESSAGE[ErrorCategory.INTERNAL_ERROR],
                category=ErrorCategory.INTERNAL_ERROR.value,
                trace_id=identity.trace_id,
            )
            payload = json.dumps(asdict(body)).encode()
            await send_wrapper(
                {
                    "type": "http.response.start",
                    "status": status_code,
                    "headers": [(b"content-type", b"application/json")],
                }
            )
            await send({"type": "http.response.body", "body": payload})
        finally:
            duration_ms = (time.perf_counter() - start) * 1000.0
            route = scope.get("route")
            route_label = getattr(route, "path", None) or UNMATCHED_ROUTE
            result = self._classify_result(status_code, category_header)
            self.registry.record(
                LatencySample(
                    route=route_label,
                    result=result,
                    duration_ms=duration_ms,
                    trace_source=identity.source.value,
                )
            )
            try:
                token.var.reset(token)
            except ValueError:
                pass  # 上下文已随任务结束，重置无意义

    @staticmethod
    def _classify_result(status_code: int, category_header: bytes | None) -> Result:
        """依据内部错误类别头（稳定）与 HTTP 状态码确定指标结果维度。"""
        if category_header is not None:
            return _CATEGORY_TO_RESULT.get(category_header, Result.INTERNAL_ERROR)
        if 200 <= status_code < 300:
            return Result.SUCCESS
        if 400 <= status_code < 500:
            return Result.CLIENT_ERROR
        return Result.INTERNAL_ERROR
