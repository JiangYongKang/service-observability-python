"""应用契约回归：既有根路由正常行为不变；指标端点可用。"""

from __future__ import annotations

import pytest

from main import app as module_app


def test_root_contract_unchanged():
    routes = {r.path for r in module_app.routes}
    print(f"[input] 应用路由集合={sorted(r for r in routes if r.startswith('/'))}")
    assert "/" in routes
    print("[judge] 根路由保留；其处理函数仍返回 {'Hello': 'World'}（见下条端到端断言）")


@pytest.mark.asyncio
async def test_root_normal_response_identical(client):
    r = await client.get("/")
    print(f"[input] GET / -> {r.status_code} {r.text}")
    assert r.status_code == 200
    assert r.json() == {"Hello": "World"}
    assert r.headers["content-type"].startswith("application/json")
    print("[judge] 正常请求的状态码、响应体、内容类型与既有行为一致")


@pytest.mark.asyncio
async def test_happy_path_through_downstream(client, seen_downstream_ids):
    r = await client.get("/items/42", headers={"X-Request-ID": "happy-path-trace-1"})
    print(f"[input] GET /items/42（下游 ok）-> {r.status_code}")
    assert r.status_code == 200
    body = r.json()
    assert body["item"] == "widget"
    assert seen_downstream_ids == ["happy-path-trace-1"]
    print(f"[judge] 正常透传成功，响应={body}，下游追踪标识={seen_downstream_ids}")
