"""In-memory request metrics with bounded label cardinality.

Labels are deliberately low-cardinality: HTTP method, the *route template*
(e.g. ``/users/{id}``, never the concrete id) and a coarse outcome class.
Concrete paths, trace ids, exception messages and other high-cardinality
values never become labels.

If more than ``max_series`` distinct label sets ever appear (normally a sign
of cardinality abuse such as raw paths leaking into the route label), the
collector does **not** drop the sample and does **not** fail the request:
further samples are folded into a single ``method="" route="other"`` catch-all
series and a degradation counter is incremented, which is observable on the
metrics endpoint.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum

# Latency bucket upper edges in seconds; an implicit +Inf bucket follows.
BUCKET_EDGES: tuple[float, ...] = (0.05, 0.1, 0.25, 0.5, 1.0)
MAX_SERIES = 50
FALLBACK_ROUTE = "other"


class Outcome(str, Enum):
    """Low-cardinality request result dimension."""

    SUCCESS = "success"
    CLIENT_ERROR = "client_error"
    DEPENDENCY_FAILURE = "dependency_failure"
    SERVER_ERROR = "server_error"


@dataclass(frozen=True, slots=True)
class MetricLabels:
    method: str
    route: str
    outcome: Outcome


@dataclass(slots=True)
class Series:
    count: int = 0
    total_seconds: float = 0.0
    max_seconds: float = 0.0
    # One counter per edge plus the trailing +Inf bucket.
    buckets: list[int] = field(
        default_factory=lambda: [0] * (len(BUCKET_EDGES) + 1)
    )


def _bucket_index(duration_seconds: float) -> int:
    """Cumulative-histogram convention: index of the first edge >= sample.

    Buckets are cumulative (Prometheus style): bucket ``le_0.1`` counts every
    sample with duration <= 0.1s. The trailing +Inf bucket always equals the
    series count.
    """
    for i, edge in enumerate(BUCKET_EDGES):
        if duration_seconds <= edge:
            return i
    return len(BUCKET_EDGES)


class MetricsCollector:
    """Thread-safe collector for request counts and latency."""

    def __init__(self, max_series: int = MAX_SERIES) -> None:
        self._lock = threading.Lock()
        self._max_series = max_series
        self._series: dict[MetricLabels, Series] = {}
        # Number of samples folded into the catch-all series.
        self.degraded_records = 0

    def record(self, labels: MetricLabels, duration_seconds: float) -> None:
        """Record one request.

        Degrades unknown label sets beyond ``max_series`` into a single
        catch-all series; never drops the sample and never raises.
        """
        if duration_seconds < 0:
            duration_seconds = 0.0
        with self._lock:
            series = self._series.get(labels)
            if series is None:
                if len(self._series) >= self._max_series:
                    # Cardinality guard: fold into one shared series.
                    labels = MetricLabels("", FALLBACK_ROUTE, labels.outcome)
                    series = self._series.setdefault(labels, Series())
                    self.degraded_records += 1
                else:
                    series = Series()
                    self._series[labels] = series
            series.count += 1
            series.total_seconds += duration_seconds
            if duration_seconds > series.max_seconds:
                series.max_seconds = duration_seconds
            # Cumulative buckets: increment every edge >= the sample.
            idx = _bucket_index(duration_seconds)
            for j in range(idx, len(series.buckets)):
                series.buckets[j] += 1

    def snapshot(self) -> dict:
        """Return a JSON-serialisable snapshot of every series."""
        with self._lock:
            items = []
            for labels, series in self._series.items():
                items.append(
                    {
                        "labels": {
                            "method": labels.method,
                            "route": labels.route,
                            "outcome": labels.outcome.value,
                        },
                        "count": series.count,
                        "latency_seconds": {
                            "sum": round(series.total_seconds, 6),
                            "max": round(series.max_seconds, 6),
                            "avg": round(
                                series.total_seconds / series.count, 6
                            )
                            if series.count
                            else 0.0,
                            "buckets": {
                                **{
                                    f"le_{edge}": value
                                    for edge, value in zip(
                                        BUCKET_EDGES, series.buckets[:-1]
                                    )
                                },
                                "+Inf": series.buckets[-1],
                            },
                        },
                    }
                )
            return {
                "series": items,
                "max_series": self._max_series,
                "degraded_records": self.degraded_records,
            }

    def reset(self) -> None:
        """Clear all series (used by tests)."""
        with self._lock:
            self._series.clear()
            self.degraded_records = 0
