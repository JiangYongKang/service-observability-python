"""Structured logging: JSON shape, trace-id coverage, secret containment."""
from __future__ import annotations

import io
import json
import logging

import httpx
from fastapi.testclient import TestClient

from obs.app import create_app
from obs.client import DownstreamClient
from obs.logging_setup import JsonFormatter, TraceContextFilter
from obs.metrics import MetricsCollector

SECRET = "log-secret-hostname-42"


def _capture():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(TraceContextFilter())
    root = logging.getLogger("obs")
    root.addHandler(handler)
    return stream, handler, root


def _lines(stream):
    return [json.loads(line) for line in stream.getvalue().splitlines() if line.strip()]


def test_every_request_log_line_is_structured_and_carries_trace_id():
    tid = "ab" * 16
    stream, handler, root = _capture()
    try:
        with TestClient(create_app(collector=MetricsCollector())) as c:
            r = c.get("/users/4242", headers={"X-Trace-Id": tid})
            assert r.status_code == 200
    finally:
        root.removeHandler(handler)

    lines = _lines(stream)
    assert lines, "no log output captured"
    for line in lines:
        print(f"[判定依据] level={line['level']} event={line.get('event')} trace_id={line['trace_id']}")
        assert isinstance(line, dict)
        assert line["trace_id"] == tid
        assert "message" in line and "level" in line
    events = {line.get("event") for line in lines}
    assert "read_user" in events
    assert "request_completed" in events
    # high-cardinality value appears in logs (that is allowed) but not metrics
    completed = [l for l in lines if l.get("event") == "request_completed"][0]
    assert completed["route"] == "/users/{user_id}"
    assert completed["status_code"] == 200
    assert "duration_ms" in completed


def test_downstream_detail_only_in_server_logs_never_response():
    stream, handler, root = _capture()
    app = create_app(
        client=DownstreamClient(
            transport=httpx.MockTransport(
                lambda r: (_ for _ in ()).throw(httpx.ConnectError(SECRET))
            )
        ),
        collector=MetricsCollector(),
    )
    try:
        with TestClient(app) as c:
            r = c.get("/quote", headers={"X-Trace-Id": "cd" * 16})
    finally:
        root.removeHandler(handler)

    assert r.status_code == 502
    assert SECRET not in r.text
    lines = _lines(stream)
    error_lines = [l for l in lines if l.get("error_category") == "connection_error"]
    assert error_lines, f"expected categorized error log, got {[l.get('event') for l in lines]}"
    for line in error_lines:
        print(f"[判定依据] 服务端错误日志 event={line.get('event')} category={line['error_category']}")
        assert line["trace_id"] == "cd" * 16
        # exception detail is allowed server-side and stays there
        if "exc_info" in line:
            assert SECRET in line["exc_info"]
