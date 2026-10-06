"""聚合入口的真实运行路径：默认即多依赖并发扇出，打哪些下游由配置
声明、使用方可按名选择。

与 test_aggregation.py 的区别：那里通过测试夹具直接注入依赖集合来压测
引擎；本文件验证的是*正常调用*路径——不手工构造依赖列表，聚合路由
自身从注册表（环境/状态配置）解析扇出集合，确保"一次请求对多个下游
并发扇出"在真实链路上跑得起、可配置、可选择。
"""

from __future__ import annotations

import time

import httpx
import pytest

from observability.registry import (
    DEFAULT_DEPENDENCY_CONFIG,
    DependencyRegistry,
    UnknownDependencySelectionError,
    parse_dependency_config,
)
from observability.metrics import registry as metrics_registry
from tests.conftest import make_async_downstream_handler


def build_real_client(*, handler, dep_registry: DependencyRegistry | None = None):
    """只注入下游客户端与注册表（或不注入注册表走默认），不注入依赖列表。"""
    from observability.app import create_app
    from observability.downstream import DownstreamClient

    application = create_app()
    application.state.downstream_client = DownstreamClient(
        "http://downstream", transport=httpx.MockTransport(handler)
    )
    if dep_registry is not None:
        application.state.aggregate_registry = dep_registry
    transport = httpx.ASGITransport(app=application)
    return httpx.AsyncClient(transport=transport, base_url="http://testserver")


def test_default_config_declares_multiple_distinct_downstreams():
    specs = parse_dependency_config(DEFAULT_DEPENDENCY_CONFIG)
    names = [s.name for s in specs]
    paths = [s.path for s in specs]
    print(f"[input] 默认配置 {DEFAULT_DEPENDENCY_CONFIG}")
    print(f"[judge] 解析出 {len(specs)} 个依赖：{names} -> {paths}")
    assert len(specs) >= 2, "默认必须声明多个下游，正常调用才会真正并发扇出"
    assert len(set(names)) == len(names) and len(set(paths)) == len(paths)


def test_malformed_config_entries_are_skipped_not_fatal(caplog):
    # 非法名（含空格/斜杠）、重名、非法路径（协议相对）、空条目混杂
    raw = "ok-a:/a, bad name:/x, dup:/d1, dup:/d2, bad://host/p, ok_b:/b, , trailing:/ok"
    specs = parse_dependency_config(raw)
    names = [s.name for s in specs]
    print(f"[input] 含非法名/重名/非法路径/空条目的配置")
    print(f"[judge] 保留的合法依赖={names}")
    assert names == ["ok-a", "dup", "ok_b", "trailing"]
    assert all(s.path.startswith("/") for s in specs)


@pytest.mark.asyncio
async def test_real_aggregate_fans_out_to_multiple_downstreams_by_default():
    """核心回归：不注入依赖列表，默认调用即并发打到多个下游。"""
    seen: dict[str, list[str]] = {}
    specs = parse_dependency_config(DEFAULT_DEPENDENCY_CONFIG)
    handler = make_async_downstream_handler(
        path_scenarios={}, path_delays={s.path: 0.05 for s in specs}, seen=seen
    )
    async with build_real_client(handler=handler) as c:
        start = time.perf_counter()
        r = await c.get("/aggregate", headers={"X-Request-ID": "real-fanout-0001"})
        elapsed = time.perf_counter() - start

    body = r.json()
    dep_names = list(body["dependencies"])
    serial_floor = 0.05 * len(specs)
    print(f"[input] {len(specs)} 个默认依赖各延迟 0.05s，串行下限 {serial_floor:.2f}s；实际 {elapsed:.2f}s")
    print(f"[judge] 实际扇出下游={sorted(seen)}，响应依赖顺序={dep_names}，结论={body['status']}")
    assert r.status_code == 200
    assert body["status"] == "ok"
    # 真实打到了多个、且是全部默认下游
    assert len(seen) == len(specs) >= 2
    assert set(seen) == {s.path for s in specs}
    # 并发：接近最慢的单个依赖，而非各依赖之和
    assert elapsed < serial_floor, "必须并发扇出，而非串行逐个调用"
    # 追踪标识：下游收到的与响应头/响应体一致
    assert r.headers["x-request-id"] == "real-fanout-0001"
    assert body["trace_id"] == "real-fanout-0001"
    assert {tid for ids in seen.values() for tid in ids} == {"real-fanout-0001"}
    # 结果按注册表声明顺序，互不混淆
    assert dep_names == [s.name for s in specs]


@pytest.mark.asyncio
async def test_caller_can_select_a_subset_of_downstreams():
    reg = DependencyRegistry.from_config("alpha:/a,beta:/b,gamma:/g")
    seen: dict[str, list[str]] = {}
    handler = make_async_downstream_handler(seen=seen)
    async with build_real_client(handler=handler, dep_registry=reg) as c:
        # 选择顺序与声明顺序不同且含重复；逗号与重复参数两种写法
        r1 = await c.get(
            "/aggregate?deps=gamma,alpha,alpha",
            headers={"X-Request-ID": "real-select-0001"},
        )
        r2 = await c.get(
            "/aggregate?deps=beta&deps=alpha",
            headers={"X-Request-ID": "real-select-0002"},
        )
    b1, b2 = r1.json(), r2.json()
    print(f"[input] deps=gamma,alpha,alpha 与 deps=beta&deps=alpha")
    print(f"[judge] 响应1顺序={list(b1['dependencies'])}；响应2顺序={list(b2['dependencies'])}")
    print(f"[judge] 下游实际被调路径={sorted(seen)}")
    assert list(b1["dependencies"]) == ["alpha", "gamma"], "按声明顺序、去重"
    assert list(b2["dependencies"]) == ["alpha", "beta"], "重复参数合并、按声明顺序"
    assert set(seen) == {"/a", "/b", "/g"}
    assert b1["trace_id"] == "real-select-0001" and b2["trace_id"] == "real-select-0002"


@pytest.mark.asyncio
async def test_unknown_dependency_selection_is_deterministic_400():
    reg = DependencyRegistry.from_config("alpha:/a,beta:/b")
    seen: dict[str, list[str]] = {}
    handler = make_async_downstream_handler(seen=seen)
    async with build_real_client(handler=handler, dep_registry=reg) as c:
        r = await c.get(
            "/aggregate?deps=alpha,../../etc,evil&deps=nope",
            headers={"X-Request-ID": "real-badsel-0001"},
        )
        metrics = (await c.get("/metrics")).json()

    body = r.json()
    print(f"[input] 选择含未声明名与路径穿越样式输入 deps=alpha,../../etc,evil,nope")
    print(f"[judge] {r.status_code} {body}；下游实际调用={seen}")
    assert r.status_code == 400
    assert body["category"] == "unknown_dependency"
    assert body["trace_id"] == "real-badsel-0001"
    assert r.headers["x-request-id"] == "real-badsel-0001"
    # 只回显稳定白名单，不回显原始恶意输入；无任何下游被真正调用
    assert body["available_dependencies"] == ["alpha", "beta"]
    assert "etc" not in r.text and "evil" not in r.text and "nope" not in r.text
    assert seen == {}
    # 归类为客户端错误，且无逐依赖样本产生（扇出未发生）
    overall = {(e["route"], e["result"]) for e in metrics["requests_total"]}
    assert ("/aggregate", "client_error") in overall
    assert metrics["dependency_requests_total"] == []


def test_registry_select_raises_on_unknown_and_reports_available():
    reg = DependencyRegistry.from_config("alpha:/a,beta:/b")
    try:
        reg.select(["alpha", "ghost"])
    except UnknownDependencySelectionError as exc:
        assert exc.unknown == ("ghost",)
        assert exc.available == ("alpha", "beta")
        print(f"[judge] 未知选择异常携带 unknown={exc.unknown} available={exc.available}")
    else:  # pragma: no cover - 防御性断言
        pytest.fail("未知依赖名必须抛出 UnknownDependencySelectionError")
