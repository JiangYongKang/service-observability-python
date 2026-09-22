"""True async-loop concurrency: contextvars isolation across interleaved tasks."""
from __future__ import annotations

import asyncio

import httpx

from obs.app import create_app
from obs.metrics import MetricsCollector

app = create_app(collector=MetricsCollector())


async def _run_concurrent(n: int):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        async def one(i: int) -> tuple[int, str, dict]:
            tid = f"{i + 1:032x}"
            resp = await client.get(f"/async-check/{i}", headers={"X-Trace-Id": tid})
            return i, tid, resp

        results = await asyncio.gather(*(one(i) for i in range(n)))
    return results


def test_async_interleaved_requests_keep_distinct_trace_ids():
    # Run without pytest-asyncio: drive an event loop explicitly.
    results = asyncio.run(_run_concurrent(40))
    echo_ids: set[str] = set()
    for i, tid, resp in results:
        assert resp.status_code == 200, (i, resp.status_code, resp.text)
        body = resp.json()
        print(f"[输入] check={i} trace={tid} [判定依据] body_trace={body['trace_id']} stable={body['stable']}")
        assert body["stable"] is True
        assert body["trace_id"] == tid
        assert resp.headers["x-trace-id"] == tid
        echo_ids.add(resp.headers["x-trace-id"])
    assert len(echo_ids) == 40
