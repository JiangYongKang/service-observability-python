"""Service-boundary behaviour: error responses, metrics, headers."""
from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

from obs.app import create_app
from obs.client import DownstreamClient
from obs.metrics import MetricsCollector

SECRET = "very-secret-upstream-detail-9000"


def app_for(handler):
    collector = MetricsCollector()
    app = create_app(
        client=DownstreamClient(transport=httpx.MockTransport(handler)),
        collector=collector,
    )
    return TestClient(app), collector


HTTP_CASES = [
    ("timeout", lambda r: (_ for _ in ()).throw(httpx.ReadTimeout(SECRET)), 504, "timeout"),
    ("connection", lambda r: (_ for _ in ()).throw(httpx.ConnectError(SECRET)), 502, "connection_error"),
    ("upstream_5xx", lambda r: httpx.Response(503, text=SECRET), 502, "upstream_5xx"),
    ("upstream_4xx", lambda r: httpx.Response(404, text=SECRET), 502, "upstream_4xx"),
    ("bad_response", lambda r: httpx.Response(200, content=b"<<<"), 502, "bad_response"),
]


@pytest.mark.parametrize("name,handler,http_status,category", HTTP_CASES, ids=[c[0] for c in HTTP_CASES])
def test_quote_error_boundary(name, handler, http_status, category):
    client, collector = app_for(handler)
    with client as c:
        r = c.get("/quote", headers={"X-Trace-Id": "f" * 32})
    print(f"[输入] {name} [判定依据] http={r.status_code} body={r.text}")
    assert r.status_code == http_status
    body = r.json()
    assert body["error"]["code"] == "dependency_failure"
    assert body["error"]["category"] == category
    assert body["error"]["message"] == "downstream dependency request failed"
    assert SECRET not in r.text
    assert "Traceback" not in r.text
    # trace headers survive error responses
    assert r.headers["x-trace-id"] == "f" * 32
    assert r.headers["x-trace-source"] == "upstream"
    # the failing request is still counted exactly once
    snap = None
    with client as c2:
        snap = c2.get("/internal/metrics").json()
    series = {
        (s["labels"]["route"], s["labels"]["outcome"]): s["count"]
        for s in snap["series"]
    }
    print(f"[判定依据] 指标序列={series}")
    assert series[("/quote", "server_error")] == 1


def test_quote_success_boundary():
    client, _ = app_for(lambda r: httpx.Response(200, json={"quote": "ok"}))
    with client as c:
        r = c.get("/quote")
        assert r.status_code == 200
        assert r.json()["quote"] == "ok"


def test_root_behavior_unchanged():
    with TestClient(create_app(collector=MetricsCollector())) as c:
        r = c.get("/")
    assert r.status_code == 200
    assert r.json() == {"Hello": "World"}


def test_trace_id_propagated_to_downstream():
    captured = {}

    def handler(request: httpx.Request):
        captured["trace"] = request.headers.get("x-trace-id")
        return httpx.Response(200, json={"quote": "ok"})

    client, _ = app_for(handler)
    with client as c:
        r = c.get("/quote", headers={"X-Trace-Id": "e" * 32})
    print(f"[判定依据] 下游收到 X-Trace-Id={captured['trace']}")
    assert r.status_code == 200
    assert captured["trace"] == "e" * 32


def test_invalid_trace_is_counted_as_client_error():
    client, collector = app_for(lambda r: httpx.Response(200, json={"quote": "ok"}))
    with client as c:
        r = c.get("/quote", headers={"X-Trace-Id": "$$$"})
        assert r.status_code == 400
        snap = c.get("/internal/metrics").json()
    series = {
        (s["labels"]["route"], s["labels"]["outcome"]): s["count"]
        for s in snap["series"]
    }
    print(f"[判定依据] 非法标识请求指标={series}")
    # rejected before routing: recorded under the low-cardinality unmatched tag
    assert series[("unmatched", "client_error")] == 1
    assert "$$$" not in str(snap)
