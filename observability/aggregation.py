"""聚合查询：并发调用多个下游依赖，在受控总时长内收口并合并结果。

判定与口径约定（详见 docs/observability.md）：

* 每个依赖调用透传本次请求的追踪标识，逐依赖记录指标；
* 每个依赖的结果独立归因（沿用 errors 模块的稳定类别），判定只依据
  异常类型与响应状态/形态，不依赖任何错误文案；
* 整体结论确定性三选一：``ok``（全部成功）/ ``partial``（部分成功）/
  ``failed``（全部失败），只取决于各依赖的成败计数，与依赖返回快慢、
  完成顺序无关；
* 个别依赖不返回时，整次聚合在 ``total_timeout`` 内收口，未完成的依赖
  记为 ``downstream_timeout``。
"""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

from observability.downstream import DownstreamClient
from observability.errors import DownstreamError, ErrorCategory
from observability.logging_setup import get_logger
from observability.metrics import (
    DependencySample,
    MetricsRegistry,
    Result,
    registry as default_registry,
)

logger = get_logger("observability.aggregation")

#: 聚合总时长上限默认值（秒），可被应用配置覆盖
DEFAULT_TOTAL_TIMEOUT_SECONDS = 3.0

#: 稳定错误类别 → 指标结果维度（封闭映射，不依赖任何文案）
_CATEGORY_TO_RESULT = {
    ErrorCategory.DOWNSTREAM_TIMEOUT: Result.DOWNSTREAM_TIMEOUT,
    ErrorCategory.DOWNSTREAM_CONNECTION: Result.DOWNSTREAM_CONNECTION,
    ErrorCategory.DOWNSTREAM_STATUS: Result.DOWNSTREAM_STATUS,
    ErrorCategory.DOWNSTREAM_INVALID_RESPONSE: Result.DOWNSTREAM_INVALID,
    ErrorCategory.INTERNAL_ERROR: Result.INTERNAL_ERROR,
}


class DependencyStatus(str, Enum):
    """单个依赖的调用结果，取值封闭。"""

    OK = "ok"
    ERROR = "error"


class AggregateVerdict(str, Enum):
    """整次聚合的结论，取值封闭、判定确定。"""

    OK = "ok"  # 全部依赖成功
    PARTIAL = "partial"  # 部分成功、部分失败
    FAILED = "failed"  # 全部失败


@dataclass(frozen=True, slots=True)
class DependencySpec:
    """一个下游依赖的声明：稳定名称（进指标标签）+ 相对路径。"""

    name: str
    path: str


#: 依赖名允许的字符集：小写字母、数字、下划线、连字符
_DEP_NAME_PATTERN = re.compile(r"^[a-z0-9_-]{2,32}$")

#: 单条依赖声明（name=path）中 name 与 path 的分隔符
_DECLARATION_SEPARATOR = "="


def _validate_dependency_name(name: str) -> None:
    """校验依赖名是否为低基数、可作指标标签的稳定标识。"""
    if not _DEP_NAME_PATTERN.match(name):
        raise ValueError(
            f"invalid dependency name {name!r}: expected 2-32 chars of "
            "lowercase letters, digits, '_' or '-'"
        )


def _validate_dependency_path(path: str) -> None:
    """校验依赖路径：必须为以 ``/`` 开头的站相对路径。"""
    if not path.startswith("/"):
        raise ValueError(f"invalid dependency path {path!r}: must start with '/'")


def parse_dependency_catalog(raw: str) -> list[DependencySpec]:
    """解析逗号分隔的依赖声明目录（``name=path,name=path``）。

    声明式、低基数：名称只允许小写字母/数字/下划线/连字符，路径必须以
    ``/`` 开头；名称重复或条目非法直接抛 :class:`ValueError`（启动期
    fail fast），避免脏配置进入运行路径。
    """
    catalog: list[DependencySpec] = []
    seen: set[str] = set()
    for chunk in raw.split(","):
        declaration = chunk.strip()
        if not declaration:
            continue
        if _DECLARATION_SEPARATOR not in declaration:
            raise ValueError(
                f"invalid dependency declaration {declaration!r}: expected 'name=path'"
            )
        name, path = declaration.split(_DECLARATION_SEPARATOR, 1)
        name, path = name.strip(), path.strip()
        _validate_dependency_name(name)
        _validate_dependency_path(path)
        if name in seen:
            raise ValueError(f"duplicate dependency name {name!r}")
        seen.add(name)
        catalog.append(DependencySpec(name=name, path=path))
    return catalog


def select_dependencies(
    catalog: list[DependencySpec], selected: list[str] | None
) -> list[DependencySpec]:
    """按使用方选择的名称从目录中挑出依赖，保持目录声明顺序。

    * ``selected`` 为空（未传选择）时返回整个目录（默认全扇出）；
    * 名称必须在目录内、且为合法低基数名称，否则抛 :class:`ValueError`；
    * 重复选择自动去重，只扇出一次。
    """
    if not selected:
        return list(catalog)
    by_name = {dep.name: dep for dep in catalog}
    chosen: list[DependencySpec] = []
    for name in selected:
        _validate_dependency_name(name)
        if name not in by_name:
            raise ValueError(f"unknown dependency {name!r}")
        if name not in {dep.name for dep in chosen}:
            chosen.append(by_name[name])
    # 以目录声明顺序返回，结论与完成顺序、选择顺序均无关
    return [dep for dep in catalog if dep.name in {d.name for d in chosen}]


@dataclass(slots=True)
class DependencyOutcome:
    """单个依赖的收口结果：成功带数据，失败带稳定类别。"""

    name: str
    status: DependencyStatus
    data: dict[str, Any] | None = None
    category: ErrorCategory | None = None
    duration_ms: float = 0.0


@dataclass(frozen=True, slots=True)
class AggregateResult:
    """整次聚合的收口结果：结论 + 按声明顺序排列的逐依赖结果。"""

    verdict: AggregateVerdict
    outcomes: tuple[DependencyOutcome, ...]


class AggregationEngine:
    """并发扇出调用各依赖，在总时长上限内收口并给出确定性结论。"""

    def __init__(
        self,
        client: DownstreamClient,
        dependencies: list[DependencySpec],
        *,
        total_timeout: float = DEFAULT_TOTAL_TIMEOUT_SECONDS,
        metrics_registry: MetricsRegistry | None = None,
    ) -> None:
        self._client = client
        self._dependencies = list(dependencies)
        self._total_timeout = total_timeout
        self._registry = metrics_registry or default_registry

    async def aggregate(self) -> AggregateResult:
        """并发调用全部依赖，收口后按声明顺序返回逐依赖结果与整体结论。"""
        if not self._dependencies:
            return AggregateResult(AggregateVerdict.OK, ())

        start = time.perf_counter()
        tasks = {
            asyncio.ensure_future(self._call(dep)): dep for dep in self._dependencies
        }
        done, pending = await asyncio.wait(tasks, timeout=self._total_timeout)
        elapsed_ms = (time.perf_counter() - start) * 1000.0

        timed_out: set[str] = set()
        for task in pending:
            dep = tasks[task]
            timed_out.add(dep.name)
            task.cancel()
            logger.warning(
                "dependency did not finish within aggregate budget",
                extra={"component": "aggregation", "dependency": dep.name},
            )
        if pending:
            # 等待取消落定，避免悬挂任务；CancelledError 由 gather 收集
            await asyncio.gather(*pending, return_exceptions=True)

        # 先按依赖对象索引已完成结果，再按声明顺序收集，与完成顺序无关
        result_by_dep = {
            id(dep): task.result() for task, dep in tasks.items() if task in done
        }
        outcomes: list[DependencyOutcome] = []
        for dep in self._dependencies:
            if dep.name in timed_out:
                outcomes.append(
                    DependencyOutcome(
                        name=dep.name,
                        status=DependencyStatus.ERROR,
                        category=ErrorCategory.DOWNSTREAM_TIMEOUT,
                        duration_ms=elapsed_ms,
                    )
                )
            else:
                outcomes.append(result_by_dep[id(dep)])

        for outcome in outcomes:
            self._record_metric(outcome)
        return AggregateResult(self._classify(outcomes), tuple(outcomes))

    async def _call(self, dep: DependencySpec) -> DependencyOutcome:
        """调用单个依赖并归因；任何异常都收敛为稳定类别，不外溢。"""
        start = time.perf_counter()
        try:
            data = await self._client.get_json(dep.path)
        except DownstreamError as exc:
            duration_ms = (time.perf_counter() - start) * 1000.0
            logger.warning(
                "dependency call failed",
                extra={
                    "component": "aggregation",
                    "dependency": dep.name,
                    "category": exc._category.value,
                    "upstream_status": exc.upstream_status,
                },
            )
            return DependencyOutcome(
                name=dep.name,
                status=DependencyStatus.ERROR,
                category=exc._category,
                duration_ms=duration_ms,
            )
        except Exception:
            # 未预料异常同样归因到封闭类别，不向外抛出、不泄露细节
            duration_ms = (time.perf_counter() - start) * 1000.0
            logger.error(
                "dependency call raised unexpected error",
                extra={"component": "aggregation", "dependency": dep.name},
                exc_info=True,
            )
            return DependencyOutcome(
                name=dep.name,
                status=DependencyStatus.ERROR,
                category=ErrorCategory.INTERNAL_ERROR,
                duration_ms=duration_ms,
            )
        duration_ms = (time.perf_counter() - start) * 1000.0
        return DependencyOutcome(
            name=dep.name,
            status=DependencyStatus.OK,
            data=data,
            duration_ms=duration_ms,
        )

    def _record_metric(self, outcome: DependencyOutcome) -> None:
        """逐依赖记录请求量与延迟；标签只含稳定依赖名与封闭结果枚举。"""
        if outcome.status is DependencyStatus.OK:
            result = Result.SUCCESS
        else:
            # category 一定存在（ERROR 路径均赋值），缺省兜底为内部错误
            result = _CATEGORY_TO_RESULT.get(
                outcome.category or ErrorCategory.INTERNAL_ERROR,
                Result.INTERNAL_ERROR,
            )
        self._registry.record_dependency(
            DependencySample(
                dependency=outcome.name,
                result=result,
                duration_ms=outcome.duration_ms,
            )
        )

    @staticmethod
    def _classify(outcomes: list[DependencyOutcome]) -> AggregateVerdict:
        """整体结论只取决于成败计数：全成功 ok / 有成功有失败 partial / 全失败 failed。"""
        succeeded = sum(1 for o in outcomes if o.status is DependencyStatus.OK)
        if succeeded == len(outcomes):
            return AggregateVerdict.OK
        if succeeded == 0:
            return AggregateVerdict.FAILED
        return AggregateVerdict.PARTIAL
