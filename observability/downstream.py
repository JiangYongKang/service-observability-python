"""下游依赖客户端：把超时/连接失败/非成功状态/非法内容映射为稳定错误类别。

归因规则（仅依据异常类型与响应的状态/形态，不读消息文本）：

* ``httpx.TimeoutException``（Connect/Read/PoolTimeout）→
  :class:`DownstreamTimeoutError`（``downstream_timeout``）；
* 其余 ``httpx.TransportError``（NetworkError 家族、协议错误）以及
  ``httpx.RequestError`` 中未能完成请求的情形 →
  :class:`DownstreamConnectionError`（``downstream_connection``）；
* 响应状态码非 2xx → :class:`DownstreamStatusError`（``downstream_status``），
  类别上携带脱敏的状态码；
* 2xx 但 body 不是合法 JSON、不是对象、或不符合结构约定 →
  :class:`DownstreamInvalidResponseError`（``downstream_invalid_response``）。

请求自动透传当前上下文的追踪标识（``X-Request-ID``）。
"""

from __future__ import annotations

from typing import Any

import httpx

from observability.context import TRACE_HEADER, current_trace_id
from observability.errors import (
    DownstreamConnectionError,
    DownstreamInvalidResponseError,
    DownstreamStatusError,
    DownstreamTimeoutError,
)
from observability.logging_setup import get_logger

logger = get_logger("observability.downstream")

DEFAULT_TIMEOUT_SECONDS = 2.0


class DownstreamClient:
    """对 httpx.AsyncClient 的薄封装，负责追踪头透传与异常归因。"""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            transport=transport,
        )

    async def get_json(self, path: str) -> dict[str, Any]:
        """发起 GET，校验状态并解析/校验 JSON 载荷，失败抛出分类异常。"""
        headers: dict[str, str] = {}
        trace_id = current_trace_id()
        if trace_id is not None:
            headers[TRACE_HEADER] = trace_id

        try:
            response = await self._client.get(path, headers=headers)
        except httpx.TimeoutException:
            # ConnectTimeout/ReadTimeout/PoolTimeout 全部归入超时
            logger.warning("downstream timeout", extra={"component": "downstream", "path": path})
            raise DownstreamTimeoutError() from None
        except httpx.TransportError:
            # 连接失败/DNS/拒绝/重置/远端协议错误，统一归入连接类
            logger.warning("downstream connection failure", extra={"component": "downstream", "path": path})
            raise DownstreamConnectionError() from None
        except httpx.RequestError:
            # 其它请求级失败同样不具备可用响应，归入连接类
            logger.warning("downstream request failure", extra={"component": "downstream", "path": path})
            raise DownstreamConnectionError() from None

        trace_suffix = {"trace_id": trace_id or "-"}
        if not (200 <= response.status_code < 300):
            logger.warning(
                "downstream non-success status",
                extra={
                    "component": "downstream",
                    "path": path,
                    "upstream_status": response.status_code,
                    **trace_suffix,
                },
            )
            raise DownstreamStatusError(response.status_code)

        try:
            payload = response.json()
        except ValueError:
            # httpx 对非 JSON / 编码错误抛 json.JSONDecodeError（ValueError 子类）
            logger.warning(
                "downstream body is not valid json",
                extra={"component": "downstream", "path": path, **trace_suffix},
            )
            raise DownstreamInvalidResponseError("not_json") from None

        return self._validate_payload(payload, path, trace_suffix)

    def _validate_payload(
        self, payload: Any, path: str, trace_suffix: dict[str, str]
    ) -> dict[str, Any]:
        """校验响应内容结构：必须为 JSON 对象，且 item 字段为非空字符串。"""
        if not isinstance(payload, dict):
            logger.warning(
                "downstream json payload is not an object",
                extra={"component": "downstream", "path": path, **trace_suffix},
            )
            raise DownstreamInvalidResponseError("not_object")
        item = payload.get("item")
        if not isinstance(item, str) or item == "":
            logger.warning(
                "downstream json payload missing required string field 'item'",
                extra={"component": "downstream", "path": path, **trace_suffix},
            )
            raise DownstreamInvalidResponseError("missing_item")
        return payload

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> DownstreamClient:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.aclose()
