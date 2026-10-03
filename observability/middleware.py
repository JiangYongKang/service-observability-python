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
5. 兜底收尾：未预期异常若逃逸出路由/异常中间件（Starlette 的
   ``ServerErrorMiddleware`` 在用户中间件之外，其生成的响应不会经过
   本中间件），由本中间件直接生成固定结构的 500 错误体——保证无论
   请求在哪个阶段结束，响应头、日志与指标上的追踪标识都对得上，
   内部类别头也绝不外透。
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
                raise  # 响应已开始，无法替换为兜底错误体，交给外层
            logger.error(
                "unhandled exception",
                extra={
                    "component": "middleware",
                    "path": scope.get("path", "-"),
                    "trace_id": identity.trace_id,
                },
                exc_info=True,
            )
            await self._send_fallback_error(send_wrapper, identity.trace_id)
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
    async def _send_fallback_error(send: Send, trace_id: str) -> None:
        """生成未预期异常的兜底响应：固定结构错误体 + 内部类别头。

        经 send_wrapper 发出，因此追踪响应头照常注入、内部类别头照常
        剥离并用于指标归类；错误体不含任何底层细节，trace_id 与响应头、
        日志、指标上的标识一致。
        """
        category = ErrorCategory.INTERNAL_ERROR
        body = ErrorBody(
            error=CATEGORY_MESSAGE[category],
            category=category.value,
            trace_id=trace_id,
        )
        payload = json.dumps(asdict(body)).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 500,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(payload)).encode()),
                    (ERROR_CATEGORY_HEADER.lower().encode(), category.value.encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": payload})

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
