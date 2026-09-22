"""Metric semantics: low-cardinality labels and bounded degradation."""
from __future__ import annotations

import threading

from fastapi.testclient import TestClient

from obs.app import create_app
from obs.metrics import (
    FALLBACK_ROUTE,
    MetricLabels,
    MetricsCollector,
    Outcome,
)


def find(snapshot, route, outcome=None, method="GET"):
    hits = [
        s for s in snapshot["series"]
        if s["labels"]["route"] == route
        and s["labels"]["method"] == method
        and (outcome is None or s["labels"]["outcome"] == outcome)
    ]
    assert len(hits) == 1, f"series {route}/{outcome} not unique: {hits}"
    return hits[0]


class TestCollector:
    def test_counts_and_latency_aggregation(self):
        c = MetricsCollector()
        labels = MetricLabels("GET", "/x", Outcome.SUCCESS)
        c.record(labels, 0.01)
        c.record(labels, 0.3)
        c.record(labels, 2.0)
        s = find(c.snapshot(), "/x")
        print(f"[判定依据] series={s}")
        assert s["count"] == 3
        assert s["latency_seconds"]["max"] == 2.0
        assert s["latency_seconds"]["buckets"]["le_0.05"] == 1
        assert s["latency_seconds"]["buckets"]["le_0.5"] == 2
        assert s["latency_seconds"]["buckets"]["+Inf"] == 3

    def test_degradation_folds_instead_of_dropping(self):
        max_series = 3
        c = MetricsCollector(max_series=max_series)
        # First 3 distinct label sets are kept.
        for i in range(3):
            c.record(MetricLabels("GET", f"/r{i}", Outcome.SUCCESS), 0.01)
        # Every further distinct set is folded into one catch-all series.
        for i in range(3, 23):
            c.record(MetricLabels("GET", f"/r{i}", Outcome.SUCCESS), 0.01)
        snap = c.snapshot()
        print(f"[判定依据] series 数={len(snap['series'])} degraded={snap['degraded_records']}")
        assert len(snap["series"]) == max_series + 1
        assert snap["degraded_records"] == 20
        fallback = find(snap, FALLBACK_ROUTE, Outcome.SUCCESS, method="")
        assert fallback["count"] == 20  # nothing dropped, request unaffected
        # Further samples still accumulate into catch-all keyed by outcome.
        c.record(MetricLabels("POST", "/totally/new", Outcome.SERVER_ERROR), 0.5)
        snap2 = c.snapshot()
        assert snap2["degraded_records"] == 21
        assert find(snap2, FALLBACK_ROUTE, Outcome.SUCCESS, method="")["count"] == 20
        assert find(snap2, FALLBACK_ROUTE, Outcome.SERVER_ERROR, method="")["count"] == 1

    def test_concurrent_records_are_not_lost(self):
        c = MetricsCollector()
        labels = MetricLabels("GET", "/c", Outcome.SUCCESS)

        def worker():
            for _ in range(1000):
                c.record(labels, 0.01)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        total = sum(s["count"] for s in c.snapshot()["series"])
        print(f"[判定依据] 期望 8000，实际 {total}")
        assert total == 8000


class TestHttpMetrics:
    def _client(self):
        return TestClient(create_app())

    def test_route_template_used_not_concrete_path(self):
        with self._client() as c:
            for uid in ("aaa", "bbb", "ccc"):
                r = c.get(f"/users/{uid}")
                assert r.status_code == 200
            snap = c.get("/internal/metrics").json()
        user_series = [
            s for s in snap["series"] if s["labels"]["route"] == "/users/{user_id}"
        ]
        print(f"[判定依据] /users 系列={user_series}")
        assert len(user_series) == 1
        assert user_series[0]["count"] == 3
        # No concrete id / trace id / raw path appears in any label.
        raw = str(snap)
        for leaked in ("aaa", "bbb", "ccc"):
            assert leaked not in raw

    def test_outcome_dimensions(self):
        with self._client() as c:
            c.get("/")                       # 200 success
            c.get("/does-not-exist")         # 404 client_error
            c.get("/users/1")                # 200 success
            snap = c.get("/internal/metrics").json()
        outcomes = {
            (s["labels"]["route"], s["labels"]["outcome"]): s["count"]
            for s in snap["series"]
        }
        print(f"[判定依据] outcome 分布={outcomes}")
        assert outcomes[("/", "success")] == 1
        assert outcomes[("unmatched", "client_error")] == 1
        assert outcomes[("/users/{user_id}", "success")] == 1
