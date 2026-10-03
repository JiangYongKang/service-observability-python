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

from dataclasses import dataclass
from enum import Enum
from typing import Any

from observability.downstream import DownstreamClient
from observability.errors import ErrorCategory

#: 聚合总时长上限默认值（秒），可被应用配置覆盖
DEFAULT_TOTAL_TIMEOUT_SECONDS = 3.0


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
    ) -> None:
        self._client = client
        self._dependencies = list(dependencies)
        self._total_timeout = total_timeout

    async def aggregate(self) -> AggregateResult:
        """执行一次聚合（骨架：尚未实现扇出逻辑）。"""
        raise NotImplementedError
