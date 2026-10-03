"""聚合查询：并发扇出、追踪透传、逐依赖归因、整体结论与兜底收尾。"""

from __future__ import annotations

import asyncio
import time

import pytest

from observability.aggregation import DependencySpec
from observability.metrics import registry
from tests.conftest import build_aggregate_client, make_async_downstream_handler

DEPS_ABC = [
    DependencySpec("alpha", "/dep-a"),
    DependencySpec("beta", "/dep-b"),
    DependencySpec("gamma", "/dep-c"),
]


@pytest.mark.asyncio
async def test_concurrent_fanout_merges_distinct_results():
    seen: dict[str, list[str]] = {}
    handler = make_async_downstream_handler(
        path_delays={"/dep-a": 0.05, "/dep-b": 0.1, "/dep-c": 0.15},
        seen=seen,
    )
    async with build_aggregate_client(dependencies=DEPS_ABC, handler=handler) as c:
        start = time.perf_counter()
        r = await c.get("/aggregate", headers={"X-Request-ID": "agg-trace-fanout"})
        elapsed = time.perf_counter() - start

    print(f"[input] 3 个依赖分别延迟 0.05/0.1/0.15s，串行需 >=0.30s；实际耗时 {elapsed:.2f}s")
    assert r.status_code == 200
    body = r.json()
    print(f"[judge] 响应={body}")
    assert elapsed < 0.30, "并发扇出：总耗时应接近最慢依赖而非各依赖之和"
    assert body["status"] == "ok"
    assert body["trace_id"] == "agg-trace-fanout"
    assert r.headers["x-request-id"] == "agg-trace-fanout"
    # 各依赖结果互不混淆，且按声明顺序呈现
    assert list(body["dependencies"]) == ["alpha", "beta", "gamma"]
    assert body["dependencies"]["alpha"]["data"]["item"] == "widget@/dep-a"
    assert body["dependencies"]["beta"]["data"]["item"] == "widget@/dep-b"
    assert body["dependencies"]["gamma"]["data"]["item"] == "widget@/dep-c"
    # 每个依赖收到的追踪标识与对外回传的一致
    print(f"[judge] 下游实际收到的标识={seen}")
    assert seen == {
        "/dep-a": ["agg-trace-fanout"],
        "/dep-b": ["agg-trace-fanout"],
        "/dep-c": ["agg-trace-fanout"],
    }


@pytest.mark.asyncio
async def test_concurrent_requests_do_not_cross_trace_ids():
    handler = make_async_downstream_handler(
        path_delays={"/dep-a": 0.02, "/dep-b": 0.05, "/dep-c": 0.08},
        echo_trace_id=True,
    )
    trace_ids = [f"agg-trace-iso-{i:04d}" for i in range(5)]
    async with build_aggregate_client(dependencies=DEPS_ABC, handler=handler) as c:
        responses = await asyncio.gather(
            *(c.get("/aggregate", headers={"X-Request-ID": tid}) for tid in trace_ids)
        )

    print(f"[input] 5 个并发聚合请求，标识={trace_ids}")
    for tid, r in zip(trace_ids, responses):
        body = r.json()
        echoed = {dep["data"]["echo_id"] for dep in body["dependencies"].values()}
        print(f"[judge] 请求 {tid}: 响应头={r.headers['x-request-id']} 体={body['trace_id']} 下游回显={echoed}")
        assert r.headers["x-request-id"] == tid
        assert body["trace_id"] == tid
        # 该请求的所有依赖看到的都是它自己的标识，不串号
        assert echoed == {tid}


@pytest.mark.asyncio
async def test_partial_failure_deterministic_regardless_of_completion_order():
    async def run(fail_delay: float, ok_delay: float) -> dict:
        handler = make_async_downstream_handler(
            path_scenarios={"/dep-b": "status"},
            path_delays={"/dep-a": ok_delay, "/dep-b": fail_delay},
        )
        async with build_aggregate_client(
            dependencies=[DependencySpec("alpha", "/dep-a"), DependencySpec("beta", "/dep-b")],
            handler=handler,
        ) as c:
            r = await c.get("/aggregate", headers={"X-Request-ID": "agg-trace-order"})
        return r

    r_fast_fail = await run(fail_delay=0.01, ok_delay=0.10)
    r_slow_fail = await run(fail_delay=0.10, ok_delay=0.01)
    print("[input] 场景1: 失败依赖先完成；场景2: 成功依赖先完成")
    for r in (r_fast_fail, r_slow_fail):
        body = r.json()
        print(f"[judge] status={r.status_code} body={body}")
        assert r.status_code == 200
        assert body["status"] == "partial"
        assert body["dependencies"]["alpha"]["status"] == "ok"
        assert body["dependencies"]["beta"] == {
            "status": "error",
            "category": "downstream_status",
        }
        # 不泄露上游原文/地址等底层细节
        assert "secret" not in r.text and "prod:5432" not in r.text
    print("[judge] 两种完成顺序下整体结论与逐依赖归因完全一致")


@pytest.mark.asyncio
async def test_all_failed_is_distinct_from_partial():
    handler = make_async_downstream_handler(
        path_scenarios={"/dep-a": "timeout", "/dep-b": "conn", "/dep-c": "status"},
    )
    async with build_aggregate_client(dependencies=DEPS_ABC, handler=handler) as c:
        r = await c.get("/aggregate", headers={"X-Request-ID": "agg-trace-allbad"})
        metrics = (await c.get("/metrics")).json()

    body = r.json()
    print(f"[input] 3 个依赖分别超时/连接失败/非成功状态 -> {r.status_code} {body}")
    assert r.status_code == 502
    assert body["status"] == "failed"
    assert body["trace_id"] == "agg-trace-allbad"
    assert body["dependencies"]["alpha"]["category"] == "downstream_timeout"
    assert body["dependencies"]["beta"]["category"] == "downstream_connection"
    assert body["dependencies"]["gamma"]["category"] == "downstream_status"
    assert "x-error-category" not in r.headers  # 内部类别头不透出
    overall = {
        (e["route"], e["result"]) for e in metrics["requests_total"]
    }
    print(f"[judge] 整体指标维度={sorted(overall)}")
    assert ("/aggregate", "aggregate_failed") in overall
    assert ("/aggregate", "partial_failure") not in overall
    per_dep = {(e["dependency"], e["result"]) for e in metrics["dependency_requests_total"]}
    print(f"[judge] 逐依赖指标维度={sorted(per_dep)}")
    assert ("alpha", "downstream_timeout") in per_dep
    assert ("beta", "downstream_connection") in per_dep
    assert ("gamma", "downstream_status") in per_dep


@pytest.mark.asyncio
async def test_total_duration_bounded_when_dependency_hangs():
    handler = make_async_downstream_handler(path_scenarios={"/dep-c": "hang"})
    async with build_aggregate_client(
        dependencies=DEPS_ABC, handler=handler, total_timeout=0.3
    ) as c:
        start = time.perf_counter()
        r = await c.get("/aggregate", headers={"X-Request-ID": "agg-trace-hang"})
        elapsed = time.perf_counter() - start
        metrics = (await c.get("/metrics")).json()

    body = r.json()
    print(f"[input] gamma 依赖悬挂（30s 不返回），聚合总时长上限 0.3s；实际耗时 {elapsed:.2f}s")
    print(f"[judge] 响应={body}")
    assert elapsed < 2.0, "整次请求必须在受控总时长内收口，不能被悬挂依赖拖死"
    assert r.status_code == 200
    assert body["status"] == "partial"
    assert body["dependencies"]["alpha"]["status"] == "ok"
    assert body["dependencies"]["beta"]["status"] == "ok"
    assert body["dependencies"]["gamma"] == {
        "status": "error",
        "category": "downstream_timeout",
    }
    per_dep = {(e["dependency"], e["result"]) for e in metrics["dependency_requests_total"]}
    print(f"[judge] 逐依赖指标维度={sorted(per_dep)}")
    assert ("gamma", "downstream_timeout") in per_dep


@pytest.mark.asyncio
async def test_per_dependency_metrics_and_cardinality_degradation():
    from observability.metrics import MAX_DEPENDENCY_CARDINALITY, OVERFLOW_DEPENDENCY

    dep_count = MAX_DEPENDENCY_CARDINALITY + 5
    deps = [DependencySpec(f"dep{i:02d}", f"/dep-{i:02d}") for i in range(dep_count)]
    handler = make_async_downstream_handler(path_scenarios={"/dep-00": "status"})
    async with build_aggregate_client(dependencies=deps, handler=handler) as c:
        r = await c.get("/aggregate", headers={"X-Request-ID": "agg-trace-card"})
        metrics = (await c.get("/metrics")).json()

    print(f"[input] {dep_count} 个依赖（上限 {MAX_DEPENDENCY_CARDINALITY}），dep00 返回非成功状态")
    card = metrics["dependency_cardinality"]
    print(f"[judge] 依赖基数信息={card}")
    assert card["degraded"] is True
    assert card["overflow_samples"] == dep_count - MAX_DEPENDENCY_CARDINALITY

    counts = metrics["dependency_requests_total"]
    total = sum(e["count"] for e in counts)
    print(f"[judge] 逐依赖计数条目数={len(counts)} 合计={total}")
    # 样本不丢：合计等于发出的依赖调用数；溢出样本合并进固定占位
    assert total == dep_count
    overflow = [e for e in counts if e["dependency"] == OVERFLOW_DEPENDENCY]
    assert overflow and overflow[0]["count"] == dep_count - MAX_DEPENDENCY_CARDINALITY
    # 逐依赖延迟同样按 依赖+结果 聚合
    lat = metrics["dependency_latency_ms"]
    assert any(e["dependency"] == "dep00" and e["result"] == "downstream_status" for e in lat)
    assert all(e["count"] >= 1 and e["sum_ms"] >= 0 for e in lat)
    # 高基数信息（追踪标识、真实路径）绝不进入标签
    labels = " ".join(e["dependency"] for e in counts)
    assert "agg-trace-card" not in labels and "/dep-" not in labels
    assert r.status_code == 200 and r.json()["status"] == "partial"


@pytest.mark.asyncio
async def test_fallback_when_merge_stage_raises(monkeypatch, caplog):
    import observability.app as app_module

    def boom(_result):
        raise RuntimeError("simulated merge crash with /etc/passwd and 10.0.0.9:6379")

    monkeypatch.setattr(app_module, "_render_aggregate_response", boom)
    handler = make_async_downstream_handler()
    async with build_aggregate_client(dependencies=DEPS_ABC, handler=handler) as c:
        with caplog.at_level("ERROR", logger="observability.middleware"):
            r = await c.get("/aggregate", headers={"X-Request-ID": "agg-trace-boom"})
        metrics = (await c.get("/metrics")).json()

    body = r.json()
    print(f"[input] 结果合并阶段抛出未预料异常 -> {r.status_code} {body}")
    assert r.status_code == 500
    assert body["category"] == "internal_error"
    # 兜底路径上，响应头 / 响应体 / 日志 / 指标的追踪标识一致
    assert r.headers["x-request-id"] == "agg-trace-boom"
    assert r.headers["x-trace-source"] == "upstream"
    assert body["trace_id"] == "agg-trace-boom"
    error_logs = [
        rec for rec in caplog.records
        if rec.name == "observability.middleware" and rec.message == "unhandled exception"
    ]
    assert error_logs and all(rec.trace_id == "agg-trace-boom" for rec in error_logs)
    overall = {(e["route"], e["result"], e["trace"]) for e in metrics["requests_total"]}
    print(f"[judge] 整体指标维度={sorted(overall)}")
    assert ("/aggregate", "internal_error", "upstream") in overall
    # 内部串联信息不透出：无类别头、无堆栈/地址/内部细节
    assert "x-error-category" not in r.headers
    assert "/etc/passwd" not in r.text and "10.0.0.9" not in r.text
    assert "RuntimeError" not in r.text


@pytest.mark.asyncio
async def test_unexpected_dependency_error_is_contained():
    async def handler(request):
        if request.url.path == "/dep-b":
            raise ValueError("unexpected library state db=prod:5432")
        return httpx.Response(200, json={"item": "widget"})

    import httpx

    async with build_aggregate_client(dependencies=DEPS_ABC, handler=handler) as c:
        r = await c.get("/aggregate", headers={"X-Request-ID": "agg-trace-unexp"})

    body = r.json()
    print(f"[input] beta 依赖抛出非下游分类的未预料异常 -> {r.status_code} {body}")
    assert r.status_code == 200
    assert body["status"] == "partial"
    # 未预料异常收敛为封闭类别，不向外抛出、不泄露细节
    assert body["dependencies"]["beta"] == {
        "status": "error",
        "category": "internal_error",
    }
    assert body["dependencies"]["alpha"]["status"] == "ok"
    assert "prod:5432" not in r.text and "ValueError" not in r.text
