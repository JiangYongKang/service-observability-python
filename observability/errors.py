"""下游错误的稳定分类、异常类型与对外错误模型。

分类只依据异常的*类型*（超时/连接）与响应的*状态码/内容形态*，绝不
依据异常消息文本——因此底层库升级措辞不会改变归因结论。

对外错误体固定为 ``{"error", "category", "trace_id"}``，不含主机、端口、
URL、堆栈等底层细节；这些细节只写入服务端结构化日志。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from fastapi import status

#: 响应头：路由处理在响应上打类别，中间件据此把结果写入指标
ERROR_CATEGORY_HEADER = "X-Error-Category"


class ErrorCategory(str, Enum):
    """稳定的错误类别。判定依据是异常/响应的类型与状态码，不依赖消息文本。"""

    DOWNSTREAM_TIMEOUT = "downstream_timeout"
    DOWNSTREAM_CONNECTION = "downstream_connection"
    DOWNSTREAM_STATUS = "downstream_status"
    DOWNSTREAM_INVALID_RESPONSE = "downstream_invalid_response"
    INTERNAL_ERROR = "internal_error"


class DownstreamError(Exception):
    """所有可归因下游错误的基类，携带稳定类别与脱敏上下文。"""

    category: ErrorCategory = ErrorCategory.INTERNAL_ERROR

    def __init__(
        self, category: ErrorCategory, *, upstream_status: int | None = None
    ) -> None:
        # 固定文案，不拼接任何底层信息，避免消息文本影响分类或外泄细节
        super().__init__(f"downstream request failed: {category.value}")
        self._category = category
        self._upstream_status = upstream_status

    @property
    def upstream_status(self) -> int | None:
        return self._upstream_status


class DownstreamTimeoutError(DownstreamError):
    """下游在允许时间内未响应（httpx.TimeoutException）。"""

    category = ErrorCategory.DOWNSTREAM_TIMEOUT

    def __init__(self) -> None:
        super().__init__(ErrorCategory.DOWNSTREAM_TIMEOUT)


class DownstreamConnectionError(DownstreamError):
    """无法建立到下游的连接（DNS、拒绝连接、连接重置等）。"""

    category = ErrorCategory.DOWNSTREAM_CONNECTION

    def __init__(self) -> None:
        super().__init__(ErrorCategory.DOWNSTREAM_CONNECTION)


class DownstreamStatusError(DownstreamError):
    """下游可达且响应合法，但返回非成功状态码。"""

    category = ErrorCategory.DOWNSTREAM_STATUS

    def __init__(self, upstream_status: int) -> None:
        super().__init__(ErrorCategory.DOWNSTREAM_STATUS, upstream_status=upstream_status)


class DownstreamInvalidResponseError(DownstreamError):
    """下游返回 2xx 但内容非法（非 JSON 或结构不符合约定）。"""

    category = ErrorCategory.DOWNSTREAM_INVALID_RESPONSE

    def __init__(self, reason: str) -> None:
        # reason 仅服务端日志使用，取值由本服务代码固定产生，不来自外部输入
        super().__init__(ErrorCategory.DOWNSTREAM_INVALID_RESPONSE)
        self._reason = reason

    @property
    def reason(self) -> str:
        return self._reason


@dataclass(frozen=True, slots=True)
class ErrorBody:
    """对外错误响应体：固定结构，不含底层异常细节。"""

    error: str
    category: str
    trace_id: str


#: 错误类别到对外 HTTP 状态码的稳定映射
CATEGORY_STATUS: dict[ErrorCategory, int] = {
    ErrorCategory.DOWNSTREAM_TIMEOUT: status.HTTP_504_GATEWAY_TIMEOUT,
    ErrorCategory.DOWNSTREAM_CONNECTION: status.HTTP_502_BAD_GATEWAY,
    ErrorCategory.DOWNSTREAM_STATUS: status.HTTP_502_BAD_GATEWAY,
    ErrorCategory.DOWNSTREAM_INVALID_RESPONSE: status.HTTP_502_BAD_GATEWAY,
    ErrorCategory.INTERNAL_ERROR: status.HTTP_500_INTERNAL_SERVER_ERROR,
}

#: 面向调用方的稳定短语，不随底层实现变化
CATEGORY_MESSAGE: dict[ErrorCategory, str] = {
    ErrorCategory.DOWNSTREAM_TIMEOUT: "downstream request timed out",
    ErrorCategory.DOWNSTREAM_CONNECTION: "downstream service unreachable",
    ErrorCategory.DOWNSTREAM_STATUS: "downstream returned an error status",
    ErrorCategory.DOWNSTREAM_INVALID_RESPONSE: "downstream response is invalid",
    ErrorCategory.INTERNAL_ERROR: "internal server error",
}
