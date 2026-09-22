"""指标：路由/结果/来源分维度聚合、高基数隔离、超限降级、并发不丢数。"""

from __future__ import annotations

import asyncio

import pytest

from observability.metrics import (
    OVERFLOW_ROUTE,
    LatencySample,
    MetricsRegistry,
    Result,
)
from tests.conftest import build_client


@pytest.mark.asyncio
async def test_http_metrics_are_keyed_by_route_template_and_result():
    async_client = build_client("ok")
    async with async_client:
        await async_client.get("/items/100", headers={"X-Request-ID": "metric-trace-0001"})
        await async_client.get("/items/200", headers={"X-Request-ID": "metric-trace-0002"})
        await async_client.get("/")
        await async_client.get("/definitely/not/matched")

    from observability.metrics import registry

    snap = registry.snapshot()
    counts = {(e["route"], e["result"], e["trace"]): e["count"]
              for e in snap["requests_total"]}
    print(f"[input] 2 次不同 item_id 的成功请求 + 1 次根路由 + 1 次 404")
    print(f"[judge] 指标序列: {sorted(counts)}")
    # 两个不同 item_id 聚合到同一路由模板，不产生两条高基数序列
    assert counts[("/items/{item_id}", "success", "upstream")] == 2
    assert counts[("/", "success", "generated")] == 1
    assert counts[("__unmatched__", "client_error", "generated")] == 1
    routes = {e["route"] for e in snap["requests_total"]}
    assert "100" not in " ".join(routes) and "200" not in " ".join(routes)
    lat = [e for e in snap["request_latency_ms"]
           if e["route"] == "/items/{item_id}" and e["result"] == "success"][0]
    assert lat["count"] == 2 and lat["min_ms"] <= lat["avg_ms"] <= lat["max_ms"]
    print(f"[judge] 延迟聚合 count={lat['count']} min/avg/max={lat['min_ms']}/{lat['avg_ms']}/{lat['max_ms']}")


def test_cardinality_degradation_keeps_all_samples():
    reg = MetricsRegistry(max_route_cardinality=5)
    print("[input] 上限 5 个路由标签，依次写入 7 个不同路由各 10 个样本")
    normal, overflow = 0, 0
    for i in range(7):
        for _ in range(10):
            ok = reg.record(
                LatencySample(f"/wild/{i}", Result.SUCCESS, 1.0, trace_source="generated")
            )
            normal += ok
            overflow += not ok
    snap = reg.snapshot()
    total = sum(e["count"] for e in snap["requests_total"])
    print(f"[judge] 正常记账={normal} 降级记账={overflow} 总样本={total} degraded={snap['cardinality']}")
    assert total == 70, "降级也不能丢弃任何样本"
    assert normal == 50 and overflow == 20
    assert snap["cardinality"]["degraded"] is True
    assert snap["cardinality"]["overflow_samples"] == 20
    ov = sum(e["count"] for e in snap["requests_total"] if e["route"] == OVERFLOW_ROUTE)
    assert ov == 20
    print("[judge] 溢出维度合并进固定 __overflow__，请求侧不失败、统计不丢弃")


@pytest.mark.asyncio
async def test_concurrent_recording_loses_no_samples():
    reg = MetricsRegistry(max_route_cardinality=1000)

    async def worker():
        for _ in range(250):
            reg.record(
                LatencySample("/items/{item_id}", Result.SUCCESS, 1.0, trace_source="upstream")
            )

    await asyncio.gather(*(worker() for _ in range(20)))
    total = sum(e["count"] for e in reg.snapshot()["requests_total"])
    print(f"[input] 20 协程 x 250 次记账 = 期望 5000，实际 {total}")
    assert total == 5000
    print("[judge] 并发记账无丢失，注册表锁保证一致性")


@pytest.mark.asyncio
async def test_metrics_endpoint_exposes_snapshot_without_high_cardinality():
    async_client = build_client("status")
    async with async_client:
        await async_client.get("/items/abc-123", headers={"X-Request-ID": "metrics-endpoint-id"})
        r = await async_client.get("/metrics")
    body = r.json()
    print(f"[input] 一次下游 503 后 GET /metrics，顶层键={sorted(body)}")
    assert r.status_code == 200
    assert "requests_total" in body and "request_latency_ms" in body
    flat = str(body)
    assert "abc-123" not in flat and "metrics-endpoint-id" not in flat
    cats = {e["result"] for e in body["requests_total"]}
    assert "downstream_status" in cats
    print(f"[judge] 快照仅含路由模板/封闭结果/来源标签；结果集合={sorted(cats)}")
