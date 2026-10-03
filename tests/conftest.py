"""测试共享夹具：内存 ASGI 客户端 + 可按场景编排的模拟下游。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest

from observability.aggregation import DependencySpec
from observability.app import create_app
from observability.downstream import DownstreamClient
from observability.metrics import registry


def make_downstream_handler(
    *,
    seen_headers: list[str] | None = None,
    delay: float = 0.0,
    default: str = "ok",
):
    """构造模拟下游传输 handler。

    通过请求头 ``x-scenario`` 选择场景：ok/timeout/conn/status/invalid/
    shape/notobj。每次调用收到的 X-Request-ID 记录进 seen_headers，供断言
    追踪标识随调用链透传。
    """

    def handler(request: httpx.Request) -> httpx.Response:
        scenario = request.headers.get("x-scenario", default)
        if seen_headers is not None:
            seen_headers.append(request.headers.get("x-request-id", ""))
        if scenario == "ok":
            return httpx.Response(200, json={"item": "widget", "echo": request.url.path})
        if scenario == "status":
            # body 故意含底层敏感字样，断言不会透出到对外响应
            return httpx.Response(503, text="upstream secret db=prod:5432")
        if scenario == "invalid":
            return httpx.Response(
                200, content=b"<<<not-json>>>", headers={"content-type": "text/plain"}
            )
        if scenario == "shape":
            return httpx.Response(200, json={"item": 12345})
        if scenario == "notobj":
            return httpx.Response(200, json=["a", "b"])
        if scenario == "timeout":
            raise httpx.ReadTimeout("simulated timeout; message text must be ignored")
        if scenario == "conn":
            raise httpx.ConnectError("simulated conn refused 10.0.0.9:6379")
        raise AssertionError(f"unknown scenario: {scenario}")

    return handler


@pytest.fixture
def seen_downstream_ids() -> list[str]:
    """记录模拟下游实际收到的追踪标识。"""
    return []


def build_client(
    scenario: str = "ok",
    seen_headers: list[str] | None = None,
) -> httpx.AsyncClient:
    """构造一个挂载模拟下游的内存 ASGI 客户端（不负责关闭）。"""
    application = create_app()
    handler = make_downstream_handler(seen_headers=seen_headers, default=scenario)
    application.state.downstream_client = DownstreamClient(
        "http://downstream", transport=httpx.MockTransport(handler)
    )
    transport = httpx.ASGITransport(app=application)
    return httpx.AsyncClient(transport=transport, base_url="http://testserver")


@pytest.fixture
async def client(seen_downstream_ids) -> AsyncIterator[httpx.AsyncClient]:
    """默认成功场景的 ASGI 异步客户端。"""
    async_client = build_client("ok", seen_downstream_ids)
    async with async_client:
        yield async_client


@pytest.fixture(autouse=True)
def _reset_metrics():
    registry.reset()
    yield
    registry.reset()


def make_async_downstream_handler(
    *,
    path_scenarios: dict[str, str] | None = None,
    path_delays: dict[str, float] | None = None,
    seen: dict[str, list[str]] | None = None,
    echo_trace_id: bool = False,
):
    """构造异步模拟下游 handler（聚合场景：可挂起、按路径编排）。

    * ``path_scenarios``：路径 → 场景（ok/status/invalid/timeout/conn/hang）；
    * ``path_delays``：路径 → 响应前等待秒数（用于验证并发与收口上限）；
    * ``seen``：记录每个路径实际收到的 X-Request-ID；
    * ``echo_trace_id``：响应体回显收到的追踪标识，用于断言不串号。
    """

    async def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        trace_id = request.headers.get("x-request-id", "")
        if seen is not None:
            seen.setdefault(path, []).append(trace_id)
        scenario = (path_scenarios or {}).get(path, "ok")
        delay = (path_delays or {}).get(path, 0.0)
        if scenario == "hang":
            await asyncio.sleep(30.0)  # 远超聚合总时长上限，等待被取消
        elif delay:
            await asyncio.sleep(delay)
        if scenario in ("ok", "hang"):
            body: dict[str, object] = {"item": f"widget@{path}"}
            if echo_trace_id:
                body["echo_id"] = trace_id
            return httpx.Response(200, json=body)
        if scenario == "status":
            # body 故意含底层敏感字样，断言不会透出到对外响应
            return httpx.Response(503, text="upstream secret db=prod:5432")
        if scenario == "invalid":
            return httpx.Response(
                200, content=b"<<<not-json>>>", headers={"content-type": "text/plain"}
            )
        if scenario == "timeout":
            raise httpx.ReadTimeout("simulated timeout; message text must be ignored")
        if scenario == "conn":
            raise httpx.ConnectError("simulated conn refused 10.0.0.9:6379")
        raise AssertionError(f"unknown scenario: {scenario}")

    return handler


def build_aggregate_client(
    *,
    dependencies: list[DependencySpec],
    handler,
    total_timeout: float | None = None,
) -> httpx.AsyncClient:
    """构造挂载异步模拟下游与指定依赖集合的内存 ASGI 客户端（不负责关闭）。"""
    application = create_app()
    application.state.downstream_client = DownstreamClient(
        "http://downstream", transport=httpx.MockTransport(handler)
    )
    application.state.aggregate_dependencies = dependencies
    if total_timeout is not None:
        application.state.aggregate_total_timeout = total_timeout
    transport = httpx.ASGITransport(app=application)
    return httpx.AsyncClient(transport=transport, base_url="http://testserver")
