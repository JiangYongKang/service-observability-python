"""Trace id resolution, propagation and concurrent isolation."""
from __future__ import annotations

import asyncio
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from obs.app import create_app
from obs.logging_setup import JsonFormatter, TraceContextFilter
from obs.metrics import MetricsCollector
from obs.tracing import (
    TraceSource,
    bind_trace_id,
    current_trace_id,
    is_valid_trace_id,
    resolve_trace_id,
)

VALID = "0123456789abcdef0123456789abcdef"
VALID_UPPER = "0123456789ABCDEF0123456789ABCDEF"


class TestResolve:
    def test_missing_header_generates_id(self):
        d = resolve_trace_id(None)
        print(f"[判定依据] inbound=None -> source={d.source.value}, id={d.trace_id}")
        assert d.source is TraceSource.GENERATED
        assert is_valid_trace_id(d.trace_id)

    def test_empty_header_is_invalid(self):
        # Header present but empty/whitespace: upstream sent *something*,
        # and that something is illegal, so it is rejected (deterministic,
        # distinguishable from header-absent generation).
        for value in ("", "   "):
            d = resolve_trace_id(value)
            print(f"[输入] {value!r} [判定依据] source={d.source.value}")
            assert d.source is TraceSource.INVALID
            assert is_valid_trace_id(d.trace_id)

    @pytest.mark.parametrize("bad", [
        "short",
        VALID_UPPER,            # uppercase not accepted
        VALID + "00",           # too long
        "g" * 32,               # non-hex
        "../etc/passwd",
        VALID[:-1] + " ",
        "123,456",
    ])
    def test_illegal_values_distinguishable(self, bad):
        d = resolve_trace_id(bad)
        print(f"[输入] {bad!r} [判定依据] source={d.source.value}")
        assert d.source is TraceSource.INVALID
        # illegal inbound value is never echoed
        assert is_valid_trace_id(d.trace_id)
        assert d.trace_id != bad

    def test_valid_value_echoed(self):
        d = resolve_trace_id(VALID)
        assert d.source is TraceSource.UPSTREAM
        assert d.trace_id == VALID


class TestContextPropagation:
    def test_bind_and_reset(self):
        assert current_trace_id() is None
        with bind_trace_id(VALID):
            assert current_trace_id() == VALID
        assert current_trace_id() is None

    def test_async_tasks_isolated_and_stable(self):
        seen: dict[int, str] = {}

        async def worker(i: int):
            tid = f"{i:032x}"
            with bind_trace_id(tid):
                # every await inside this task must keep the same id
                for _ in range(5):
                    await asyncio.sleep(0)
                    assert current_trace_id() == tid
                seen[i] = current_trace_id()

        async def main():
            await asyncio.gather(*(worker(i) for i in range(20)))

        asyncio.run(main())
        assert len(set(seen.values())) == 20

    def test_threadpool_isolation(self):
        barrier = threading.Barrier(8)
        results: dict[int, str | None] = {}

        def worker(i: int):
            tid = f"{i + 1:032x}"
            with bind_trace_id(tid):
                barrier.wait()  # maximise interleaving
                results[i] = current_trace_id()

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(worker, range(8)))
        assert len(set(results.values())) == 8
        assert current_trace_id() is None


class TestHttpTracing:
    def _client(self):
        return TestClient(create_app(collector=MetricsCollector()))

    def test_valid_id_echoed_and_used_in_chain(self):
        with self._client() as c:
            r = c.get("/users/abc123", headers={"X-Trace-Id": VALID})
            print(f"[输入] X-Trace-Id={VALID} [判定依据] {r.status_code=} {r.headers=}")
            assert r.status_code == 200
            assert r.headers["x-trace-id"] == VALID
            assert r.headers["x-trace-source"] == "upstream"
            assert r.json()["trace_id"] == VALID

    def test_missing_id_is_generated(self):
        with self._client() as c:
            r = c.get("/users/abc123")
            generated = r.headers["x-trace-id"]
            print(f"[输入] 无 header [判定依据] source={r.headers['x-trace-source']} id={generated}")
            assert r.headers["x-trace-source"] == "generated"
            assert is_valid_trace_id(generated)
            assert r.json()["trace_id"] == generated

    def test_invalid_id_rejected_before_app(self):
        with self._client() as c:
            r = c.get("/", headers={"X-Trace-Id": "not-an-id!!"})
            print(f"[输入] 非法 id [判定依据] status={r.status_code} body={r.text}")
            assert r.status_code == 400
            assert r.headers["x-trace-source"] == "invalid"
            assert r.json()["error"]["code"] == "invalid_trace_id"
            # illegal value is not echoed back
            assert r.headers.get("x-trace-id") is None

    def test_concurrent_requests_do_not_mix_ids(self):
        n = 30
        with self._client() as c:
            def call(i: int):
                tid = f"{i + 1:032x}"
                r = c.get(f"/users/u{i}", headers={"X-Trace-Id": tid})
                return i, tid, r

            with ThreadPoolExecutor(max_workers=10) as pool:
                responses = list(pool.map(call, range(n)))

        observed = set()
        for i, tid, r in responses:
            print(f"[输入] user=u{i} trace={tid} [判定依据] echo={r.headers['x-trace-id']}")
            assert r.status_code == 200
            assert r.headers["x-trace-id"] == tid
            assert r.json()["trace_id"] == tid
            observed.add(r.headers["x-trace-id"])
        assert len(observed) == n

    def test_every_log_line_inside_request_has_trace_id(self, caplog):
        import io

        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(JsonFormatter())
        handler.addFilter(TraceContextFilter())
        root = logging.getLogger("obs")
        root.addHandler(handler)
        try:
            with self._client() as c:
                r = c.get("/users/zz", headers={"X-Trace-Id": VALID})
                assert r.status_code == 200
        finally:
            root.removeHandler(handler)
        lines = [json.loads(l) for l in stream.getvalue().splitlines()]
        assert lines, "expected structured log output"
        for line in lines:
            print(f"[判定依据] logger={line['logger']} trace_id={line['trace_id']}")
            assert line["trace_id"] == VALID
