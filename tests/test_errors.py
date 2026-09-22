"""下游异常链路：四类稳定错误类别、脱敏响应、分类不依赖消息文本。"""

from __future__ import annotations

import httpx
import pytest

from observability.errors import (
    DownstreamConnectionError,
    DownstreamInvalidResponseError,
    DownstreamStatusError,
    DownstreamTimeoutError,
)
from observability.metrics import registry
from tests.conftest import build_client

SCENARIOS = {
    "timeout": (504, "downstream_timeout", DownstreamTimeoutError),
    "conn": (502, "downstream_connection", DownstreamConnectionError),
    "status": (502, "downstream_status", DownstreamStatusError),
    "invalid": (502, "downstream_invalid_response", DownstreamInvalidResponseError),
    "shape": (502, "downstream_invalid_response", DownstreamInvalidResponseError),
    "notobj": (502, "downstream_invalid_response", DownstreamInvalidResponseError),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario,expect", SCENARIOS.items(), ids=list(SCENARIOS))
async def test_error_categories_are_distinct_and_stable(scenario, expect):
    expect_status, expect_category, expect_exc = expect
    async_client = build_client(scenario)
    print(f"[input] 下游场景={scenario}（模拟底层异常/响应），调用 /items/1")
    async with async_client:
        r = await async_client.get("/items/1")

    body = r.json()
    print(f"[judge] HTTP {r.status_code} category={body.get('category')}; "
          f"依据: 异常类型/状态码/内容形态，而非消息文本")
    assert r.status_code == expect_status
    assert body["category"] == expect_category
    assert set(body) == {"error", "category", "trace_id"}
    assert isinstance(body["error"], str) and body["error"]
    print(f"[judge] 对外错误体固定字段 {sorted(body)}，错误短语稳定: {body['error']!r}")


@pytest.mark.asyncio
async def test_response_does_not_leak_low_level_details():
    async_client = build_client("status")
    async with async_client:
        r_status = await async_client.get("/items/1")
    async_client = build_client("conn")
    async with async_client:
        r_conn = await async_client.get("/items/1")

    print("[input] 下游 503 响应体含 'secret db=prod:5432'；连接异常消息含 '10.0.0.9:6379'")
    for r in (r_status, r_conn):
        text = r.text
        assert "secret" not in text
        assert "db=prod" not in text
        assert "10.0.0.9" not in text
        assert "Traceback" not in text
        assert "x-error-category" not in {k.lower() for k in r.headers}
    print("[judge] 两个响应均不含主机/端口/堆栈/上游原文；内部 X-Error-Category 头已剥离")


@pytest.mark.asyncio
async def test_category_does_not_depend_on_message_text():
    """即使底层消息文本任意变化，分类仍只由异常类型决定。"""

    state = {"calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        state["calls"] += 1
        if state["calls"] == 1:
            # ReadTimeout 的任意非标准措辞
            raise httpx.ReadTimeout("totally different wording zzz")
        # 第二次：ConnectError 的任意措辞
        raise httpx.ConnectError("ECONNRESET / dns failure / 任意措辞")

    from observability.app import create_app
    from observability.downstream import DownstreamClient

    app = create_app()
    app.state.downstream_client = DownstreamClient(
        "http://downstream", transport=httpx.MockTransport(handler)
    )
    async_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t"
    )
    print("[input] 连续两次调用：ReadTimeout(奇怪措辞) 与 ConnectError(任意措辞)")
    try:
        async with async_client:
            r1 = await async_client.get("/items/1")
            r2 = await async_client.get("/items/1")
        assert r1.json()["category"] == "downstream_timeout"
        assert r2.json()["category"] == "downstream_connection"
        print(f"[judge] 文本任意但类别由类型稳定决定: {r1.json()['category']} / {r2.json()['category']}")
    finally:
        await app.state.downstream_client.aclose()


@pytest.mark.asyncio
async def test_metrics_record_error_categories_per_route():
    async_client = build_client("timeout")
    async with async_client:
        await async_client.get("/items/1", headers={"X-Request-ID": "trace-err-metric-1"})
    snap = registry.snapshot()
    rows = {(e["route"], e["result"], e["trace"]): e["count"]
            for e in snap["requests_total"]}
    print(f"[input] 一次下游超时请求后读取 /metrics 维度: {rows}")
    assert rows[("/items/{item_id}", "downstream_timeout", "upstream")] == 1
    # 高基数的 item_id=1 绝不能成为标签
    assert all("trace-err-metric-1" not in e["route"] for e in snap["requests_total"])
    print("[judge] 指标按路由模板+结果类别+标识来源聚合；trace 标识值本身不进入标签")
