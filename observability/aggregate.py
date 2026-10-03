"""聚合查询：并发调用多个下游依赖，按确定性规则合并为单一响应。

设计约定（详见 docs/observability.md）：

* 一次聚合请求内的每个下游调用都经 :class:`DownstreamClient` 发出，自动
  携带本次请求的追踪标识；asyncio 任务拷贝 Context，并发请求互不串号；
* 每个依赖的结果独立归因（沿用 errors 模块的稳定类别，判定只依据异常
  类型与响应状态/形态，不读任何错误文案），互不影响；
* 整体结论只取决于各依赖的*结果集合*，与完成顺序、返回快慢无关；
* 整次聚合有可控的总时长上限（``total_timeout``），到点未完成的依赖
  按 ``downstream_timeout`` 收口，个别依赖不返回不会拖死整次请求；
* 每个依赖的成败都记入逐依赖指标（``dependency`` 标签只取白名单名称）。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

from observability.context import current_trace_id
from observability.downstream import DownstreamClient
from observability.errors import DownstreamError, ErrorCategory
from observability.logging_setup import get_logger
from observability.metrics import (
    DependencySample,
    MetricsRegistry,
    Result,
    registry as default_registry,
)

logger = get_logger("observability.aggregate")

#: 整次聚合的默认总时长上限（秒），可被应用配置覆盖
DEFAULT_TOTAL_TIMEOUT_SECONDS = 3.0


class AggregateStatus(str, Enum):
    """聚合整体结论：全部成功 / 部分失败 / 全部失败，三者明显区分。"""

    OK = "ok"
    PARTIAL = "partial"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class DependencySpec:
    """一个下游依赖的调用约定：名称（白名单、可进指标标签）与路径模板。"""

    name: str
    path: str


@dataclass(frozen=True, slots=True)
class DependencyOutcome:
    """单个依赖的最终结果：成功带数据，失败带稳定类别（不含底层细节）。"""

    name: str
    ok: bool
    category: str | None  # 失败时为 ErrorCategory 值；成功为 None
    data: dict[str, Any] | None  # 成功时为下游 JSON 载荷；失败为 None


@dataclass(frozen=True, slots=True)
class AggregateReport:
    """一次聚合的完整报告：整体结论 + 按名称排序的逐依赖结果。"""

    status: AggregateStatus
    outcomes: tuple[DependencyOutcome, ...]  # 按依赖名排序，与完成顺序无关
    overall_category: str | None  # 全部失败时的整体归因；否则为 None
    trace_id: str


#: 全部失败时整体归因的固定优先级：取失败类别中优先级最高者。
#: 顺序是代码常量，与依赖完成顺序、错误文案均无关。
_CATEGORY_PRIORITY: tuple[ErrorCategory, ...] = (
    ErrorCategory.DOWNSTREAM_TIMEOUT,
    ErrorCategory.DOWNSTREAM_CONNECTION,
    ErrorCategory.DOWNSTREAM_STATUS,
    ErrorCategory.DOWNSTREAM_INVALID_RESPONSE,
    ErrorCategory.INTERNAL_ERROR,
)

#: 错误类别到指标结果维度的稳定映射（与中间件对整体请求的口径一致）
_CATEGORY_TO_RESULT: dict[ErrorCategory, Result] = {
    ErrorCategory.DOWNSTREAM_TIMEOUT: Result.DOWNSTREAM_TIMEOUT,
    ErrorCategory.DOWNSTREAM_CONNECTION: Result.DOWNSTREAM_CONNECTION,
    ErrorCategory.DOWNSTREAM_STATUS: Result.DOWNSTREAM_STATUS,
    ErrorCategory.DOWNSTREAM_INVALID_RESPONSE: Result.DOWNSTREAM_INVALID,
    ErrorCategory.INTERNAL_ERROR: Result.INTERNAL_ERROR,
}


def decide_status(outcomes: list[DependencyOutcome]) -> AggregateStatus:
    """由逐依赖结果集合确定整体结论（纯函数，与顺序无关）。

    规则固定：全部成功 → ``ok``；有成功也有失败 → ``partial``；
    全部失败 → ``failed``。
    """
    failed = sum(1 for o in outcomes if not o.ok)
    if failed == 0:
        return AggregateStatus.OK
    if failed == len(outcomes):
        return AggregateStatus.FAILED
    return AggregateStatus.PARTIAL


def decide_overall_category(outcomes: list[DependencyOutcome]) -> str | None:
    """全部失败时按固定优先级归纳整体类别（纯函数，不读错误文案）。

    存在成功依赖（整体为 ok/partial）时返回 None；否则在失败类别中按
    :data:`_CATEGORY_PRIORITY` 的固定顺序取优先级最高者。
    """
    if not outcomes or any(o.ok for o in outcomes):
        return None
    failed_categories = {
        ErrorCategory(o.category) for o in outcomes if o.category is not None
    }
    for category in _CATEGORY_PRIORITY:
        if category in failed_categories:
            return category.value
    # 防御分支：全部失败但都缺类别，正常路径不会走到
    return ErrorCategory.INTERNAL_ERROR.value


async def _call_dependency(
    spec: DependencySpec,
    client: DownstreamClient,
    metrics_registry: MetricsRegistry,
) -> DependencyOutcome:
    """调用单个依赖并归因：成败都记录逐依赖指标，异常不外抛。"""
    start = time.perf_counter()

    def record(result: Result) -> None:
        duration_ms = (time.perf_counter() - start) * 1000.0
        metrics_registry.record_dependency(DependencySample(spec.name, result, duration_ms))

    try:
        data = await client.get_json(spec.path)
    except DownstreamError as exc:
        category = exc._category
        record(_CATEGORY_TO_RESULT[category])
        logger.warning(
            "aggregate dependency failed",
            extra={
                "component": "aggregate",
                "dependency": spec.name,
                "category": category.value,
                "upstream_status": exc.upstream_status,
            },
        )
        return DependencyOutcome(spec.name, False, category.value, None)
    except Exception:
        # 未预期异常：归入内部错误，堆栈只进日志
        record(Result.INTERNAL_ERROR)
        logger.error(
            "aggregate dependency unexpected error",
            extra={"component": "aggregate", "dependency": spec.name},
            exc_info=True,
        )
        return DependencyOutcome(spec.name, False, ErrorCategory.INTERNAL_ERROR.value, None)
    record(Result.SUCCESS)
    return DependencyOutcome(spec.name, True, None, data)


async def run_aggregate(
    specs: list[DependencySpec],
    client: DownstreamClient,
    *,
    total_timeout: float = DEFAULT_TOTAL_TIMEOUT_SECONDS,
    metrics_registry: MetricsRegistry | None = None,
) -> AggregateReport:
    """并发调用各依赖并收口：总时长受控，逐依赖归因，结论确定。

    * 每个依赖一个 asyncio 任务，任务拷贝当前 Context，追踪标识随
      :class:`DownstreamClient` 自动透传，且并发请求之间互不串号；
    * ``asyncio.wait`` 在 ``total_timeout`` 到点后返回，未完成任务被取消
      并按 ``downstream_timeout`` 记为失败，整次请求在可控时长内收口；
    * 返回报告中的逐依赖结果按名称排序，结论只依赖结果集合。
    """
    registry = metrics_registry or default_registry
    trace_id = current_trace_id() or "-"

    tasks: dict[asyncio.Task[DependencyOutcome], DependencySpec] = {
        asyncio.ensure_future(_call_dependency(spec, client, registry)): spec
        for spec in specs
    }
    outcomes: list[DependencyOutcome] = []
    if tasks:
        start = time.perf_counter()
        done, pending = await asyncio.wait(tasks.keys(), timeout=total_timeout)
        for task in done:
            outcomes.append(task.result())
        for task in pending:
            task.cancel()
            spec = tasks[task]
            duration_ms = (time.perf_counter() - start) * 1000.0
            registry.record_dependency(
                DependencySample(spec.name, Result.DOWNSTREAM_TIMEOUT, duration_ms)
            )
            logger.warning(
                "aggregate dependency cancelled by overall deadline",
                extra={
                    "component": "aggregate",
                    "dependency": spec.name,
                    "category": ErrorCategory.DOWNSTREAM_TIMEOUT.value,
                    "total_timeout_ms": round(total_timeout * 1000.0, 3),
                },
            )
            outcomes.append(
                DependencyOutcome(
                    spec.name, False, ErrorCategory.DOWNSTREAM_TIMEOUT.value, None
                )
            )
        if pending:
            # 回收被取消任务，避免未消费异常告警；取消本身即时生效
            await asyncio.gather(*pending, return_exceptions=True)

    ordered = tuple(sorted(outcomes, key=lambda o: o.name))
    status = decide_status(list(ordered))
    overall_category = decide_overall_category(list(ordered))
    logger.info(
        "aggregate finished",
        extra={
            "component": "aggregate",
            "status": status.value,
            "overall_category": overall_category or "-",
            "dependencies": ",".join(
                f"{o.name}:{'ok' if o.ok else o.category}" for o in ordered
            ),
        },
    )
    return AggregateReport(status, ordered, overall_category, trace_id)
