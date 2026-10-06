"""聚合查询真实运行路径：默认多依赖并发扇出、使用方选择依赖、非法选择兜底。

与 test_aggregation.py 的区别：本组用例不注入 app.state 依赖集合，直接走
应用启动时由 AGGREGATE_DEPENDENCIES/默认目录解析出的生产配置，确保“正常
调用就扇出多个下游”，而不是只有写测试时才能构造出多依赖。
"""

from __future__ import annotations

import asyncio
import time

import pytest

from observability.app import DEFAULT_AGGREGATE_DEPENDENCIES
from tests.conftest import build_real_aggregate_client, make_async_downstream_handler

DEFAULT_PATHS = [dep.path for dep in DEFAULT_AGGREGATE_DEPENDENCIES]
DEFAULT_NAMES = [dep.name for dep in DEFAULT_AGGREGATE_DEPENDENCIES]


def test_default_catalog_has_multiple_dependencies():
    # 真实路径要扇出多下游，默认目录必须声明多于一个依赖
    print(f"[input] 生产默认依赖目录={[ (d.name, d.path) for d in DEFAULT_AGGREGATE_DEPENDENCIES]}")
    assert len(DEFAULT_AGGREGATE_DEPENDENCIES) > 1
    print(f"[judge] 默认即配置 {len(DEFAULT_AGGREGATE_DEPENDENCIES)} 个依赖，真实入口无需测试注入即可多扇出")


@pytest.mark.asyncio
async def test_default_request_fans_out_to_all_configured_downstreams():
    seen: dict[str, list[str]] = {}
    # 每个路径都给一点延迟，若被串行化总耗时将是各延迟之和
    delays = {path: 0.05 for path in DEFAULT_PATHS}
    handler = make_async_downstream_handler(path_delays=delays, seen=seen)
    async with build_real_aggregate_client(handler) as c:
        start = time.perf_counter()
        r = await c.get("/aggregate", headers={"X-Request-ID": "real-trace-fanout"})
        elapsed = time.perf_counter() - start

    body = r.json()
    print(f"[input] 不传 deps 直接 GET /aggregate，{len(DEFAULT_PATHS)} 个依赖各延迟 0.05s；实际耗时 {elapsed:.2f}s")
    print(f"[judge] 下游实际被调用路径={sorted(seen)}；响应依赖={list(body['dependencies'])}")
    assert r.status_code == 200
    assert body["status"] == "ok"
    # 关键：真实路径上每个配置的下游都被打到了
    assert sorted(seen) == sorted(DEFAULT_PATHS)
    assert list(body["dependencies"]) == DEFAULT_NAMES
    assert all(v["status"] == "ok" for v in body["dependencies"].values())
    # 并发而非串行：总耗时接近单个依赖延迟
    assert elapsed < 0.05 * len(DEFAULT_PATHS)
    # 每个下游收到的追踪标识与对外回传一致
    assert all(ids == ["real-trace-fanout"] for ids in seen.values())
    assert r.headers["x-request-id"] == "real-trace-fanout"
    print("[judge] 默认调用即多依赖并发扇出，追踪标识逐下游一致")


@pytest.mark.asyncio
async def test_concurrent_aggregate_requests_stay_isolated():
    handler = make_async_downstream_handler(
        path_delays={path: 0.02 for path in DEFAULT_PATHS}, echo_trace_id=True
    )
    trace_ids = [f"real-iso-{i:04d}" for i in range(4)]
    async with build_real_aggregate_client(handler) as c:
        responses = await asyncio.gather(
            *(c.get("/aggregate", headers={"X-Request-ID": tid}) for tid in trace_ids)
        )

    print(f"[input] 真实路径上 4 个并发聚合请求，标识={trace_ids}")
    for tid, r in zip(trace_ids, responses):
        body = r.json()
        echoed = {dep["data"]["echo_id"] for dep in body["dependencies"].values()}
        print(f"[judge] {tid}: 响应头={r.headers['x-request-id']} 体={body['trace_id']} 下游回显集合={echoed}")
        assert r.status_code == 200
        assert r.headers["x-request-id"] == tid
        assert body["trace_id"] == tid
        # 该请求的所有下游看到的都是它自己的标识，结果也只含自己的依赖
        assert echoed == {tid}
        assert list(body["dependencies"]) == DEFAULT_NAMES
    print("[judge] 并发请求间追踪标识与逐依赖结果互不串号/混淆")


@pytest.mark.asyncio
async def test_deps_query_param_selects_subset():
    seen: dict[str, list[str]] = {}
    handler = make_async_downstream_handler(seen=seen)
    chosen = [DEFAULT_NAMES[0], DEFAULT_NAMES[-1]]
    async with build_real_aggregate_client(handler) as c:
        r = await c.get(
            "/aggregate",
            params=[("deps", chosen[0]), ("deps", chosen[-1])],
            headers={"X-Request-ID": "real-trace-subset"},
        )

    body = r.json()
    expected_paths = [
        dep.path for dep in DEFAULT_AGGREGATE_DEPENDENCIES if dep.name in chosen
    ]
    print(f"[input] deps 重复传参选择 {chosen}；实际命中下游={sorted(seen)}")
    print(f"[judge] 响应依赖={list(body['dependencies'])}")
    assert r.status_code == 200
    assert sorted(seen) == sorted(expected_paths)
    assert list(body["dependencies"]) == chosen

    # 逗号分隔 + 重复选择去重，且按目录声明顺序收口
    seen.clear()
    async with build_real_aggregate_client(handler) as c:
        r2 = await c.get(
            f"/aggregate?deps={chosen[-1]},{chosen[0]},{chosen[0]}",
            headers={"X-Request-ID": "real-trace-csv"},
        )
    body2 = r2.json()
    print(f"[input] deps 逗号分隔+乱序+重复；实际命中={sorted(seen)} 响应顺序={list(body2['dependencies'])}")
    assert r2.status_code == 200
    assert sorted(seen) == sorted(expected_paths)
    assert list(body2["dependencies"]) == chosen
    print("[judge] 使用方可选部分下游；重复去重、顺序以目录声明为准")


@pytest.mark.asyncio
async def test_unknown_dependency_selection_rejected_without_downstream_call():
    seen: dict[str, list[str]] = {}
    handler = make_async_downstream_handler(seen=seen)
    metrics_before = None
    async with build_real_aggregate_client(handler) as c:
        r = await c.get(
            "/aggregate?deps=ghost", headers={"X-Request-ID": "real-trace-400"}
        )
        metrics = (await c.get("/metrics")).json()

    body = r.json()
    print(f"[input] deps=ghost（目录外）；下游是否被调用={seen}；响应={r.status_code} {body}")
    assert r.status_code == 400
    assert body == {
        "error": "invalid request",
        "category": "invalid_request",
        "trace_id": "real-trace-400",
    }
    # 拒绝路径不发起任何下游调用
    assert seen == {}
    # 追踪标识在响应头/体一致；内部类别头剥离；不回显原始输入
    assert r.headers["x-request-id"] == "real-trace-400"
    assert r.headers["x-trace-source"] == "upstream"
    assert "x-error-category" not in r.headers
    assert "ghost" not in r.text
    overall = {(e["route"], e["result"]) for e in metrics["requests_total"]}
    print(f"[judge] 整体指标维度={sorted(overall)}")
    assert ("/aggregate", "client_error") in overall
    # 逐依赖指标不应为被拒绝的选择记账
    assert metrics["dependency_requests_total"] == []
    print("[judge] 非法选择 400、不打下游、指标归 client_error、底层细节不外泄")


@pytest.mark.asyncio
async def test_partial_and_total_timeout_on_real_catalog():
    # 让第一个配置依赖返回非成功状态、最后一个悬挂；中间依赖正常
    scenarios = {DEFAULT_PATHS[0]: "status", DEFAULT_PATHS[-1]: "hang"}
    handler = make_async_downstream_handler(path_scenarios=scenarios)
    async with build_real_aggregate_client(handler, total_timeout=0.3) as c:
        start = time.perf_counter()
        r = await c.get("/aggregate", headers={"X-Request-ID": "real-trace-partial"})
        elapsed = time.perf_counter() - start
        metrics = (await c.get("/metrics")).json()

    body = r.json()
    first, last = DEFAULT_NAMES[0], DEFAULT_NAMES[-1]
    print(f"[input] {first} 返回 503、{last} 悬挂，其余正常，总时长上限 0.3s；实际 {elapsed:.2f}s")
    print(f"[judge] 响应={body}")
    assert elapsed < 2.0, "悬挂依赖不能拖死整次聚合"
    assert r.status_code == 200
    assert body["status"] == "partial"
    assert body["dependencies"][first] == {
        "status": "error",
        "category": "downstream_status",
    }
    assert body["dependencies"][last] == {
        "status": "error",
        "category": "downstream_timeout",
    }
    middle_names = DEFAULT_NAMES[1:-1]
    assert all(body["dependencies"][n]["status"] == "ok" for n in middle_names)
    # 不泄露上游原文
    assert "secret" not in r.text and "prod:5432" not in r.text
    per_dep = {(e["dependency"], e["result"]) for e in metrics["dependency_requests_total"]}
    overall = {(e["route"], e["result"]) for e in metrics["requests_total"]}
    print(f"[judge] 逐依赖指标={sorted(per_dep)}；整体指标={sorted(overall)}")
    assert (first, "downstream_status") in per_dep
    assert (last, "downstream_timeout") in per_dep
    assert ("/aggregate", "partial_failure") in overall
    print("[judge] 真实目录下部分失败与总时长收口结论稳定、逐依赖归因清晰")



def test_dependency_catalog_config_parsing():
    from observability.aggregation import (
        DependencySpec,
        parse_dependency_catalog,
    )

    print("[input] 解析 AGGREGATE_DEPENDENCIES 风格配置串 'a-one=/a, b_two=/b ,'")
    catalog = parse_dependency_catalog("a-one=/a, b_two=/b ,")
    print(f"[judge] 解析结果={[(d.name, d.path) for d in catalog]}")
    assert catalog == [
        DependencySpec("a-one", "/a"),
        DependencySpec("b_two", "/b"),
    ]
    # 空串对解析器而言是“空目录”，由装配层负责拒绝
    assert parse_dependency_catalog("") == []

    bad_inputs = ["UPPER=/x", "s=/x", "dup=/x,dup=/y", "ok=relative/path", "noseparator"]
    for raw in bad_inputs:
        with pytest.raises(ValueError):
            parse_dependency_catalog(raw)
    print(f"[judge] 非法配置 {bad_inputs} 均在启动期 fail fast")


def test_empty_catalog_is_rejected_at_assembly():
    import observability.app as app_module

    print("[input] 装配层加载空依赖目录")
    with pytest.raises(RuntimeError):
        app_module._load_default_dependencies("")
    print("[judge] 空目录在启动期被拒绝，避免聚合入口无依赖可扇出")


def test_aggregate_dependencies_env_var_configures_catalog():
    """AGGREGATE_DEPENDENCIES 环境变量在进程启动时决定真实扇出目录。"""
    import json
    import os
    import subprocess
    import sys
    from pathlib import Path

    helper = Path(__file__).with_name("_env_catalog_check.py")
    repo_root = Path(__file__).resolve().parents[1]
    env = dict(
        os.environ,
        AGGREGATE_DEPENDENCIES="svc_a=/svc-a,svc_b=/svc-b,svc_c=/svc-c",
        PYTHONPATH=str(repo_root),
    )
    proc = subprocess.run(
        [sys.executable, str(helper)], capture_output=True, text=True, env=env
    )
    print("[input] AGGREGATE_DEPENDENCIES=svc_a=/svc-a,svc_b=/svc-b,svc_c=/svc-c")
    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout.splitlines()[-1])
    print(f"[judge] 子进程真实目录={result['names']} {result['paths']}")
    assert result["names"] == ["svc_a", "svc_b", "svc_c"]
    assert result["paths"] == ["/svc-a", "/svc-b", "/svc-c"]
    print(f"[judge] deps=svc_b -> {result['subset']}；缺省 -> {result['all']}")
    assert result["subset"] == ["svc_b"]
    assert result["all"] == ["svc_a", "svc_b", "svc_c"]
