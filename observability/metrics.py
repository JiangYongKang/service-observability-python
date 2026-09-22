"""按路由、结果与标识来源分维度的请求量/延迟指标，带标签基数保护与降级。

口径（详见 docs/observability.md）：

* ``requests_total{route,result,trace}``：请求计数；
* ``request_latency_ms{route,result,trace}``：延迟的 count/sum/min/max；
* ``route`` 只允许取白名单（已知路由模板）、``__unmatched__``（未匹配
  路由）或 ``__overflow__``（基数超限降级占位），任何 path 参数/查询串/
  主机头等高基数信息都不进入标签；
* ``result`` 为封闭枚举（处理结果），``trace`` 为三值封闭枚举
  （upstream/generated/rejected，标识来源），基数固定且有界；
* 当不同 route 取值数达到 ``MAX_ROUTE_CARDINALITY`` 后，新维度不再创建，
  其样本合并进 ``__overflow__`` 并置 ``degraded=true``：统计不丢弃、
  请求也不失败，同时记录一条降级告警日志。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum

from observability.logging_setup import get_logger

logger = get_logger("observability.metrics")

#: 未匹配任何已知路由时使用的稳定占位
UNMATCHED_ROUTE = "__unmatched__"

#: 基数降级后，溢出维度合并到的占位标签
OVERFLOW_ROUTE = "__overflow__"

#: 路由标签允许的不同取值上限（含两个占位），超出即降级
MAX_ROUTE_CARDINALITY = 50


class Result(str, Enum):
    """请求处理结果维度，取值封闭、稳定，不含任何高基数信息。"""

    SUCCESS = "success"
    CLIENT_ERROR = "client_error"
    DOWNSTREAM_TIMEOUT = "downstream_timeout"
    DOWNSTREAM_CONNECTION = "downstream_connection"
    DOWNSTREAM_STATUS = "downstream_status"
    DOWNSTREAM_INVALID = "downstream_invalid"
    INTERNAL_ERROR = "internal_error"


@dataclass(frozen=True, slots=True)
class LatencySample:
    """一次请求的指标采样点。"""

    route: str
    result: Result
    duration_ms: float
    trace_source: str  # upstream / generated / rejected，封闭三值


@dataclass(slots=True)
class _LatencyAgg:
    count: int = 0
    total_ms: float = 0.0
    min_ms: float | None = None
    max_ms: float | None = None

    def observe(self, duration_ms: float) -> None:
        self.count += 1
        self.total_ms += duration_ms
        self.min_ms = duration_ms if self.min_ms is None else min(self.min_ms, duration_ms)
        self.max_ms = duration_ms if self.max_ms is None else max(self.max_ms, duration_ms)


@dataclass(slots=True)
class MetricsRegistry:
    """进程内指标注册表：请求计数 + 延迟聚合，带基数降级。

    线程安全（锁保护），可在并发/异步请求下共享。
    """

    max_route_cardinality: int = MAX_ROUTE_CARDINALITY
    _requests_total: dict[tuple[str, str, str], int] = field(default_factory=dict)
    _latency: dict[tuple[str, str, str], _LatencyAgg] = field(default_factory=dict)
    _routes: set[str] = field(default_factory=set)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _degraded: bool = False
    _overflow_samples: int = 0

    def record(self, sample: LatencySample) -> bool:
        """记录一次采样。

        返回 True 表示按原始维度正常记账；False 表示触发基数保护，样本
        已合并进 ``__overflow__``（不丢弃、不抛错）。
        """
        with self._lock:
            route, degraded = self._resolve_route(sample.route)
            key = (route, sample.result.value, sample.trace_source)
            self._requests_total[key] = self._requests_total.get(key, 0) + 1
            agg = self._latency.get(key)
            if agg is None:
                agg = _LatencyAgg()
                self._latency[key] = agg
            agg.observe(sample.duration_ms)
            if degraded:
                self._overflow_samples += 1
            return not degraded

    def _resolve_route(self, route: str) -> tuple[str, bool]:
        """归并路由标签；返回 (最终标签, 是否走了溢出降级)。调用方持锁。"""
        if route in self._routes or route in (UNMATCHED_ROUTE, OVERFLOW_ROUTE):
            self._routes.add(route)
            return route, False
        if len(self._routes) < self.max_route_cardinality:
            self._routes.add(route)
            return route, False
        if not self._degraded:
            self._degraded = True
            logger.warning(
                "metrics cardinality limit reached; degrading to overflow label",
                extra={
                    "component": "metrics",
                    "limit": self.max_route_cardinality,
                    "overflow_route": OVERFLOW_ROUTE,
                },
            )
        return OVERFLOW_ROUTE, True

    def snapshot(self) -> dict[str, object]:
        """导出当前指标快照，供读取端点与测试断言使用。"""
        with self._lock:
            requests_total = [
                {"route": route, "result": result, "trace": trace_src, "count": count}
                for (route, result, trace_src), count in sorted(self._requests_total.items())
            ]
            latency = [
                {
                    "route": route,
                    "result": result,
                    "trace": trace_src,
                    "count": agg.count,
                    "sum_ms": round(agg.total_ms, 3),
                    "min_ms": round(agg.min_ms, 3) if agg.min_ms is not None else None,
                    "max_ms": round(agg.max_ms, 3) if agg.max_ms is not None else None,
                    "avg_ms": round(agg.total_ms / agg.count, 3),
                }
                for (route, result, trace_src), agg in sorted(self._latency.items())
            ]
            return {
                "requests_total": requests_total,
                "request_latency_ms": latency,
                "cardinality": {
                    "distinct_routes": len(self._routes),
                    "limit": self.max_route_cardinality,
                    "degraded": self._degraded,
                    "overflow_samples": self._overflow_samples,
                },
            }

    def reset(self) -> None:
        """清空注册表（主要用于测试隔离）。"""
        with self._lock:
            self._requests_total.clear()
            self._latency.clear()
            self._routes.clear()
            self._degraded = False
            self._overflow_samples = 0


#: 进程级单例，供中间件与应用共享
registry = MetricsRegistry()
