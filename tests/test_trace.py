"""追踪标识：入口确定、随调用链透传、并发不串号、非法输入可区分。"""

from __future__ import annotations

import asyncio

import pytest

from observability.context import (
    TRACE_HEADER,
    TraceSource,
    current_trace_id,
    is_valid_trace_id,
    resolve_trace_id,
)
from tests.conftest import build_client


def test_validity_rules_are_deterministic():
    cases = {
        "abc-DEF_123": True,
        "a" * 8: True,
        "a" * 128: True,
        "short": False,          # 少于 8
        "a" * 129: False,        # 超过 128
        "bad id!": False,        # 含空白与感叹号
        "中文标识": False,        # 非白名单字符
        "": False,
    }
    print(f"[input] 校验用例 {cases}")
    for raw, expected in cases.items():
        got = is_valid_trace_id(raw)
        print(f"[judge] is_valid_trace_id({raw[:20]!r}) -> {got}; 依据: 长度 {len(raw)} 是否落在 8..128 且字符属白名单")
        assert got is expected


def test_resolve_sources_are_distinguishable():
    missing = resolve_trace_id(None)
    empty = resolve_trace_id("")
    good = resolve_trace_id("upstream-trace-001")
    bad = resolve_trace_id("!!非法!!")
    print(f"[input] 上游头 None/''/合法/非法 -> 来源 {[missing.source, empty.source, good.source, bad.source]}")
    assert missing.source is TraceSource.GENERATED
    assert empty.source is TraceSource.GENERATED
    assert good.source is TraceSource.UPSTREAM and good.trace_id == "upstream-trace-001"
    assert bad.source is TraceSource.REJECTED
    assert is_valid_trace_id(bad.trace_id) and bad.trace_id != "!!非法!!"
    print(f"[judge] 非法输入被替换为合法新标识 {bad.trace_id}，来源稳定为 rejected")


@pytest.mark.asyncio
async def test_response_echoes_id_and_source(client):
    r_missing = await client.get("/")
    r_upstream = await client.get("/", headers={TRACE_HEADER: "trace-from-caller-1"})
    r_bad = await client.get("/", headers={TRACE_HEADER: "bad id!"})

    print(f"[input] 三种入口头: 缺失/合法/非法")
    print(f"[judge] 缺失 -> X-Trace-Source={r_missing.headers['x-trace-source']}, id={r_missing.headers['x-request-id']}")
    assert r_missing.headers["x-trace-source"] == "generated"
    assert r_upstream.headers["x-request-id"] == "trace-from-caller-1"
    assert r_upstream.headers["x-trace-source"] == "upstream"
    assert r_bad.headers["x-trace-source"] == "rejected"
    assert r_bad.headers["x-request-id"] != "bad id!" and is_valid_trace_id(
        r_bad.headers["x-request-id"]
    )
    print("[judge] 非法头不透传，返回服务端新标识；三种情况对外可由 X-Trace-Source 区分")


@pytest.mark.asyncio
async def test_id_propagates_to_downstream(seen_downstream_ids):
    async_client = build_client("ok", seen_downstream_ids)
    async with async_client:
        r = await async_client.get(
            "/items/777", headers={TRACE_HEADER: "end-to-end-trace-id"}
        )
    print(f"[input] 入口 X-Request-ID=end-to-end-trace-id 调用 /items/777")
    assert r.status_code == 200
    assert seen_downstream_ids == ["end-to-end-trace-id"]
    print(f"[judge] 下游实际收到 X-Request-ID={seen_downstream_ids}; 响应回传={r.headers['x-request-id']}")
    assert r.headers["x-request-id"] == "end-to-end-trace-id"


@pytest.mark.asyncio
async def test_concurrent_requests_never_cross_talk(seen_downstream_ids):
    async_client = build_client("ok", seen_downstream_ids)
    n = 60

    async with async_client:

        async def one(i: int):
            tid = f"concurrent-trace-{i:04d}"
            # 响应回传的标识必须恰好是本请求的标识
            r = await async_client.get("/items/1", headers={TRACE_HEADER: tid})
            assert r.headers["x-request-id"] == tid, (tid, r.headers["x-request-id"])
            return tid

        results = await asyncio.gather(*(one(i) for i in range(n)))

    print(f"[input] 并发 {n} 个请求，各自使用唯一标识 concurrent-trace-0000..{n - 1:04d}")
    assert len(results) == n and len(set(results)) == n
    # 下游收到的标识集合与请求一一对应（完成顺序由调度决定，不作为判据）
    assert sorted(seen_downstream_ids) == sorted(results)
    assert len(set(seen_downstream_ids)) == n, "下游收到重复标识即发生串号"
    print(f"[judge] 下游收到 {len(seen_downstream_ids)} 个互不重复标识，与请求集合完全一致：无串号")
    assert current_trace_id() is None  # 请求结束后上下文不残留
