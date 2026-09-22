"""结构化日志：JSON 字段、trace_id 关联、并发不串号、异常细节只入日志。"""

from __future__ import annotations

import asyncio
import io
import json
import logging

import pytest

from observability.context import bind_trace_id
from observability.logging_setup import JsonFormatter, TraceIdFilter, configure_logging


def _capture_logs() -> io.StringIO:
    configure_logging()
    stream = io.StringIO()
    root = logging.getLogger()
    root.handlers[0].stream = stream
    return stream


def test_logs_are_json_with_trace_id():
    stream = _capture_logs()
    token = bind_trace_id("log-trace-0001")
    try:
        logging.getLogger("t").info("hello structured", extra={"k": "v"})
        line = json.loads(stream.getvalue().splitlines()[-1])
        print(f"[input] 绑定 log-trace-0001 后写一条 info；输出={line}")
        for field in ("timestamp", "level", "logger", "message", "trace_id"):
            assert field in line
        assert line["trace_id"] == "log-trace-0001"
        assert line["message"] == "hello structured" and line["k"] == "v"
        print("[judge] 单行 JSON 含固定字段与 trace_id，extra 平铺进入；判定依据：字段集合与取值")
    finally:
        token.var.reset(token)


@pytest.mark.asyncio
async def test_concurrent_logs_keep_per_request_trace_id():
    stream = _capture_logs()
    log = logging.getLogger("concurrent")

    async def worker(tid: str):
        bind_trace_id(tid)
        await asyncio.sleep(0)
        log.warning("work", extra={"unit": tid[-4:]})

    async def main():
        await asyncio.gather(*(worker(f"log-trace-{i:04d}") for i in range(30)))

    await main()
    rows = [json.loads(l) for l in stream.getvalue().splitlines() if '"concurrent"' in l]
    print(f"[input] 30 协程各自绑定标识并打一条 warning；共采集 {len(rows)} 行")
    assert len(rows) == 30
    for row in rows:
        suffix = row["unit"]
        assert row["trace_id"].endswith(suffix), row
    assert len({r["trace_id"] for r in rows}) == 30
    print("[judge] 每行 trace_id 与该协程 unit 一一匹配且互不重复：日志在并发下不串号")


@pytest.mark.asyncio
async def test_downstream_failure_logs_carry_same_trace_as_response():
    from tests.conftest import build_client

    stream = _capture_logs()
    async_client = build_client("conn")
    async with async_client:
        r = await async_client.get("/items/9", headers={"X-Request-ID": "log-err-trace-xyz"})
    trace_in_response = r.json()["trace_id"]
    app_log_lines = [
        json.loads(l) for l in stream.getvalue().splitlines()
        if '"observability.app"' in l or '"observability.downstream"' in l
    ]
    print(f"[input] 下游连接失败，请求标识 log-err-trace-xyz；响应 trace_id={trace_in_response}")
    assert trace_in_response == "log-err-trace-xyz"
    assert app_log_lines, "应当至少有一条 app/downstream 日志"
    for row in app_log_lines:
        assert row["trace_id"] == "log-err-trace-xyz"
    # 底层异常细节（堆栈）出现在日志，但不出现于对外响应
    assert any("exception" in row for row in app_log_lines)
    assert "10.0.0.9" not in r.text
    print(f"[judge] {len(app_log_lines)} 条相关日志均携带同一标识；堆栈仅入日志，响应无底层细节")
