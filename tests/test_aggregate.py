"""聚合查询：并发多依赖、逐依赖归因、总时长受控、逐依赖指标与降级、兜底收尾。

每个用例打印输入场景（[input]）与判定依据（[judge]）。
"""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from observability.app import create_app
from observability.downstream import DownstreamClient
from observability.metrics import (
    OVERFLOW_DEPENDENCY,
    DependencySample,
    MetricsRegistry,
    Result,
    registry,
)

#: 模拟下游的行为剧本：依赖名 -> (场景, 延迟秒)
Behavior = dict[str, tuple[str, float]]


def make_aggregate_client(
    behaviors: Behavior,
    seen: list[tuple[str, str]] | None = None,
    *,
    total_timeout: float | None = None,
    raise_app_exceptions: bool = True,
) -> httpx.AsyncClient:
    """构造挂载路径剧本式模拟下游的内存 ASGI 客户端（不负责关闭）。

    模拟下游按 URL 末段（依赖名）查 ``behaviors`` 决定行为；每次调用把
    （依赖名, 收到的 X-Request-ID）记入 ``seen`` 供断言追踪透传。
    场景：ok/status/invalid/timeout/conn/hang。
    """

    async def handler(request: httpx.Request) -> httpx.Response:
        dep = request.url.path.rsplit("/", 1)[-1]
        scenario, delay = behaviors[dep]
        if seen is not None:
            seen.append((dep, request.headers.get("x-request-id", "")))
        if delay:
            await asyncio.sleep(delay)
        if scenario == "ok":
            # item 值即依赖名，便于断言不同依赖的结果没有混装
            return httpx.Response(200, json={"item": dep})
        if scenario == "status":
            # body 故意含底层敏感字样，断言不会透出到对外响应
            return httpx.Response(503, text="upstream secret db=10.0.0.9:5432")
        if scenario == "invalid":
            return httpx.Response(
                200, content=b"<<<not-json>>>", headers={"content-type": "text/plain"}
            )
        if scenario == "timeout":
            raise httpx.ReadTimeout("simulated timeout; message text must be ignored")
        if scenario == "conn":
            raise httpx.ConnectError("simulated conn refused 10.0.0.9:6379")
        if scenario == "hang":
            await asyncio.sleep(30.0)  # 远超聚合总时长，靠总时长上限收口
            return httpx.Response(200, json={"item": dep})
        raise AssertionError(f"unknown scenario: {scenario}")

    application = create_app()
    application.state.downstream_client = DownstreamClient(
        "http://downstream", transport=httpx.MockTransport(handler)
    )
    if total_timeout is not None:
        application.state.aggregate_timeout = total_timeout
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(
            app=application, raise_app_exceptions=raise_app_exceptions
        ),
        base_url="http://testserver",
    )


async def test_aggregate_all_ok_merges_distinct_results():
    seen: list[tuple[str, str]] = []
    # 三个依赖故意给不同延迟，验证结果不按完成顺序混装
    behaviors: Behavior = {
        "items": ("ok", 0.03),
        "inventory": ("ok", 0.01),
        "pricing": ("ok", 0.02),
    }
    async with make_aggregate_client(behaviors, seen) as c:
        r = await c.get("/aggregate", headers={"X-Request-ID": "agg-trace-000001"})
    body = r.json()
    print(f"[input] 三依赖全部成功（延迟 10/20/30ms 乱序完成），X-Request-ID=agg-trace-000001")
    print(f"[judge] 状态={r.status_code} 整体结论={body['status']} 依赖条目={[d['name'] for d in body['dependencies']]}")
    assert r.status_code == 200
    assert body["status"] == "ok"
    # 逐依赖条目按名称排序，数据各自独立（item 值即依赖名）
    assert [d["name"] for d in body["dependencies"]] == ["inventory", "items", "pricing"]
    for entry in body["dependencies"]:
        assert entry["status"] == "ok" and entry["category"] is None
        assert entry["data"]["item"] == entry["name"], "不同依赖的结果不能混在一起"
    # 追踪标识：响应回传与每个依赖实际收到的一致
    assert r.headers["x-request-id"] == "agg-trace-000001"
    assert r.headers["x-trace-source"] == "upstream"
    assert body["trace_id"] == "agg-trace-000001"
    assert sorted(seen) == [
        ("inventory", "agg-trace-000001"),
        ("items", "agg-trace-000001"),
        ("pricing", "agg-trace-000001"),
    ]
    print(f"[judge] 每个依赖收到的标识与对外回传一致: {sorted(seen)}")


async def test_aggregate_concurrent_requests_never_cross_talk():
    seen: list[tuple[str, str]] = []
    behaviors: Behavior = {
        "items": ("ok", 0.01),
        "inventory": ("ok", 0.02),
        "pricing": ("ok", 0.01),
    }
    async with make_aggregate_client(behaviors, seen) as c:

        async def one(i: int) -> str:
            tid = f"agg-concurrent-{i:04d}"
            r = await c.get("/aggregate", headers={"X-Request-ID": tid})
            assert r.status_code == 200
            assert r.headers["x-request-id"] == tid
            assert r.json()["trace_id"] == tid
            return tid

        ids = await asyncio.gather(*(one(i) for i in range(20)))

    print(f"[input] 并发 20 个聚合请求，各自唯一标识 agg-concurrent-0000..0019")
    # 每个依赖都应恰好收到全部 20 个标识各一次，且无串号
    per_dep: dict[str, list[str]] = {}
    for dep, tid in seen:
        per_dep.setdefault(dep, []).append(tid)
    print(f"[judge] 各依赖收到的标识数: {{k: len(v) for k, v in per_dep.items()}} = "
          f"{ {k: len(v) for k, v in per_dep.items()} }")
    assert set(per_dep) == {"items", "inventory", "pricing"}
    for dep, tids in per_dep.items():
        assert sorted(tids) == sorted(ids), f"{dep} 收到的标识与请求集合不一致"
    assert len(set(seen)) == 60, "（依赖, 标识）对总数 = 3 依赖 x 20 请求"


async def test_aggregate_partial_failure_is_deterministic_and_attributed():
    async def run_once(delays: dict[str, float]) -> tuple[int, dict]:
        behaviors: Behavior = {
            "items": ("ok", delays["items"]),
            "inventory": ("timeout", delays["inventory"]),
            "pricing": ("status", delays["pricing"]),
        }
        async with make_aggregate_client(behaviors) as c:
            r = await c.get("/aggregate", headers={"X-Request-ID": "agg-partial-00001"})
        return r.status_code, r.json()

    # 同一失败集合、两种完全不同的完成顺序
    code_a, body_a = await run_once({"items": 0.03, "inventory": 0.01, "pricing": 0.02})
    code_b, body_b = await run_once({"items": 0.01, "inventory": 0.03, "pricing": 0.01})
    print(f"[input] items 成功 + inventory 超时 + pricing 返回 503，两种完成顺序各跑一次")
    print(f"[judge] 第一次={body_a['status']} 第二次={body_b['status']}（完成顺序不同，结论必须一致）")
    assert code_a == code_b == 200
    assert body_a == body_b, "结论不能随依赖返回快慢/完成顺序变化"
    assert body_a["status"] == "partial"
    per_dep = {d["name"]: d for d in body_a["dependencies"]}
    assert per_dep["items"]["status"] == "ok" and per_dep["items"]["data"]["item"] == "items"
    assert per_dep["inventory"]["category"] == "downstream_timeout"
    assert per_dep["pricing"]["category"] == "downstream_status"
    # 对外不泄露地址/上游原文等底层细节
    flat = str(body_a)
    assert "10.0.0.9" not in flat and "secret" not in flat and "5432" not in flat
    print(f"[judge] 逐依赖归因={[(d['name'], d['status'], d['category']) for d in body_a['dependencies']]}，无底层细节外泄")


async def test_aggregate_all_failed_uses_fixed_priority_category():
    behaviors: Behavior = {
        "items": ("status", 0.01),      # downstream_status
        "inventory": ("conn", 0.02),    # downstream_connection
        "pricing": ("timeout", 0.01),   # downstream_timeout（固定优先级最高）
    }
    async with make_aggregate_client(behaviors) as c:
        r = await c.get("/aggregate", headers={"X-Request-ID": "agg-failed-000001"})
    body = r.json()
    print(f"[input] 三依赖全部失败：status/conn/timeout 三类混合")
    print(f"[judge] 状态={r.status_code} 整体类别={body.get('category')}（固定优先级 timeout > connection > status）")
    assert r.status_code == 504  # downstream_timeout -> 504
    assert body["status"] == "failed"
    assert body["category"] == "downstream_timeout"
    assert body["error"] == "downstream request timed out"
    assert body["trace_id"] == "agg-failed-000001"
    assert all(d["status"] == "failed" for d in body["dependencies"])
    # 内部类别头只供中间件记账，不对外透出
    assert "x-error-category" not in r.headers
    # 整体指标结果维度与类别一致
    overall = [
        e for e in registry.snapshot()["requests_total"]
        if e["route"] == "/aggregate"
    ]
    assert overall == [
        {"route": "/aggregate", "result": "downstream_timeout", "trace": "upstream", "count": 1}
    ]
    print(f"[judge] 部分失败与全部失败明显区分（failed+504）；整体指标={overall}")


async def test_aggregate_total_timeout_bounds_whole_request():
    behaviors: Behavior = {
        "items": ("ok", 0.01),
        "inventory": ("hang", 0.0),   # 一直不返回
        "pricing": ("ok", 0.02),
    }
    start = time.perf_counter()
    async with make_aggregate_client(behaviors, total_timeout=0.3) as c:
        r = await c.get("/aggregate", headers={"X-Request-ID": "agg-timeout-00001"})
    elapsed = time.perf_counter() - start
    body = r.json()
    print(f"[input] inventory 依赖挂起不返回，聚合总时长上限 0.3s")
    print(f"[judge] 实际耗时={elapsed:.3f}s（远小于挂起的 30s），整体结论={body['status']}")
    assert elapsed < 5.0, "整次请求必须在可控总时长内收口，不能被一个依赖拖死"
    assert r.status_code == 200
    assert body["status"] == "partial"
    per_dep = {d["name"]: d for d in body["dependencies"]}
    assert per_dep["inventory"]["status"] == "failed"
    assert per_dep["inventory"]["category"] == "downstream_timeout"
    assert per_dep["items"]["status"] == "ok" and per_dep["pricing"]["status"] == "ok"
    # 超收口的依赖也记入逐依赖指标
    dep_metrics = {
        (e["dependency"], e["result"]): e["count"]
        for e in registry.snapshot()["dependency_requests_total"]
    }
    assert dep_metrics[("inventory", "downstream_timeout")] == 1
    print(f"[judge] 挂起依赖按 downstream_timeout 收口并记账: {dep_metrics}")


async def test_per_dependency_metrics_labels_and_no_high_cardinality():
    behaviors: Behavior = {
        "items": ("ok", 0.0),
        "inventory": ("invalid", 0.0),
        "pricing": ("ok", 0.0),
    }
    async with make_aggregate_client(behaviors) as c:
        await c.get("/aggregate?deps=items,inventory,pricing",
                    headers={"X-Request-ID": "agg-metrics-00001"})
        r = await c.get("/metrics")
    snap = r.json()
    dep_counts = {
        (e["dependency"], e["result"]): e["count"]
        for e in snap["dependency_requests_total"]
    }
    print(f"[input] 一次聚合（items/inventory=invalid/pricing）后读取 /metrics")
    print(f"[judge] 逐依赖计数={dep_counts}")
    assert dep_counts[("items", "success")] == 1
    assert dep_counts[("inventory", "downstream_invalid")] == 1
    assert dep_counts[("pricing", "success")] == 1
    dep_latency = {
        (e["dependency"], e["result"]) for e in snap["dependency_latency_ms"]
    }
    assert ("items", "success") in dep_latency
    # 高基数值（请求标识、参数值、真实路径）绝不进入任何标签
    flat = str(snap)
    assert "agg-metrics-00001" not in flat
    assert "/deps/items" not in flat and "deps=items" not in flat
    # 整体维度仍在：route 为模板 /aggregate
    assert any(e["route"] == "/aggregate" for e in snap["requests_total"])
    print("[judge] 逐依赖维度只含白名单依赖名+封闭结果；整体维度用路由模板；无高基数泄露")


def test_dependency_cardinality_degradation_keeps_all_samples():
    reg = MetricsRegistry(max_dependency_cardinality=3)
    print("[input] 依赖标签上限 3，依次写入 5 个不同依赖各 4 个样本")
    normal, overflow = 0, 0
    for i in range(5):
        for _ in range(4):
            ok = reg.record_dependency(
                DependencySample(f"dep-{i}", Result.SUCCESS, 1.0)
            )
            normal += ok
            overflow += not ok
    snap = reg.snapshot()
    total = sum(e["count"] for e in snap["dependency_requests_total"])
    print(f"[judge] 正常记账={normal} 降级记账={overflow} 总样本={total} "
          f"cardinality={snap['cardinality']}")
    assert total == 20, "降级也不能丢弃任何样本"
    assert normal == 12 and overflow == 8
    assert snap["cardinality"]["dependency_degraded"] is True
    assert snap["cardinality"]["dependency_overflow_samples"] == 8
    ov = sum(
        e["count"] for e in snap["dependency_requests_total"]
        if e["dependency"] == OVERFLOW_DEPENDENCY
    )
    assert ov == 8
    print("[judge] 溢出依赖合并进固定 __overflow__：样本不丢、快照可见降级")


async def test_aggregate_fallback_when_unexpected_stage_fails(monkeypatch):
    """兜底收尾：聚合在依赖调用/合并之外的未预期阶段出错时，追踪标识在
    响应头、错误体与指标结果维度上仍一致，内部细节不外透。"""
    behaviors: Behavior = {"items": ("ok", 0.0), "inventory": ("ok", 0.0), "pricing": ("ok", 0.0)}
    # ServerErrorMiddleware 生成 500 响应后会重抛异常，测试客户端不再上抛
    async with make_aggregate_client(behaviors, raise_app_exceptions=False) as c:
        # 模拟“结果合并”阶段出现未预期异常
        async def boom(*args, **kwargs):
            raise RuntimeError("merge blew up with internal path /deps/items")

        monkeypatch.setattr("observability.app.run_aggregate", boom)
        r = await c.get("/aggregate", headers={"X-Request-ID": "agg-fallback-0001"})
    body = r.json()
    print(f"[input] 合并阶段抛出未预期 RuntimeError")
    print(f"[judge] 状态={r.status_code} 错误体={body}")
    assert r.status_code == 500
    assert body["category"] == "internal_error"
    # 响应头、错误体、日志用同一标识
    assert r.headers["x-request-id"] == "agg-fallback-0001"
    assert body["trace_id"] == "agg-fallback-0001"
    # 内部异常细节（含路径）不透出；内部类别头也不外透
    assert "/deps/items" not in str(body) and "merge blew up" not in str(body)
    assert "x-error-category" not in r.headers
    overall = [
        e for e in registry.snapshot()["requests_total"]
        if e["route"] == "/aggregate"
    ]
    assert overall == [
        {"route": "/aggregate", "result": "internal_error", "trace": "upstream", "count": 1}
    ]
    print(f"[judge] 指标结果维度=internal_error 与响应一致: {overall}")


async def test_aggregate_unknown_dependency_rejected():
    behaviors: Behavior = {"items": ("ok", 0.0), "inventory": ("ok", 0.0), "pricing": ("ok", 0.0)}
    async with make_aggregate_client(behaviors) as c:
        r = await c.get("/aggregate?deps=items,unknown-dep",
                        headers={"X-Request-ID": "agg-unknown-00001"})
    print(f"[input] deps 含白名单外名称 unknown-dep -> {r.status_code}")
    assert r.status_code == 400
    assert "unknown-dep" not in r.text, "固定文案，不回显输入"
    overall = [
        e for e in registry.snapshot()["requests_total"] if e["route"] == "/aggregate"
    ]
    assert overall[0]["result"] == "client_error"
    print(f"[judge] 白名单外依赖一律 400 且记入 client_error: {overall}")
