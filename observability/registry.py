"""下游依赖注册表：声明可供聚合扇出的依赖集合，支持配置与使用方选择。

部署时通过环境变量 ``AGGREGATE_DEPENDENCIES`` 声明全部可聚合依赖；
调用 ``GET /aggregate`` 时可用 ``?deps=名称1,名称2`` 选择子集：

* 不带 ``deps`` -> 扇出全部已声明依赖（按声明顺序）；
* 名称必须命中注册表白名单，未知名称得到确定性的 400，绝不把请求
  输入拼接进真实路径或指标标签；
* 返回的依赖集合始终按注册表声明顺序排列，与选择参数的书写顺序、
  依赖返回快慢/完成顺序都无关。

名称是低基数稳定标识（唯一会进入逐依赖指标标签与选择参数的内容）；
真实下游路径只在服务内部使用，不进标签、不进对外错误信息。
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterable, Sequence

from observability.aggregation import DependencySpec
from observability.logging_setup import get_logger

logger = get_logger("observability.registry")

#: 依赖声明环境变量：``name:/path,name:/path``
DEPENDENCIES_ENV = "AGGREGATE_DEPENDENCIES"

#: 未配置环境变量时的默认多依赖集合（部署侧仍可用环境变量整体覆盖）
DEFAULT_DEPENDENCY_CONFIG = (
    "items:/items/aggregate,stock:/stock/summary,pricing:/pricing/summary"
)

#: 依赖名允许的字符集与长度（封闭白名单，保证低基数）
_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_MAX_PATH_LEN = 200


class UnknownDependencySelectionError(Exception):
    """使用方选择了注册表中不存在的依赖名（稳定归因，不依赖文案）。"""

    def __init__(self, unknown: Sequence[str], available: Sequence[str]) -> None:
        super().__init__("unknown dependency selection")
        self._unknown = tuple(unknown)
        self._available = tuple(available)

    @property
    def unknown(self) -> tuple[str, ...]:
        return self._unknown

    @property
    def available(self) -> tuple[str, ...]:
        return self._available


def _valid_name(name: str) -> bool:
    return bool(_NAME_RE.fullmatch(name))


def _valid_path(path: str) -> bool:
    # 必须是相对路径：以 / 开头、长度受限、不含空白与协议段
    return (
        path.startswith("/")
        and not path.startswith("//")
        and len(path) <= _MAX_PATH_LEN
        and not any(ch.isspace() for ch in path)
        and "://" not in path
    )


def parse_dependency_config(raw: str | None) -> list[DependencySpec]:
    """解析 ``name:/path,name:/path`` 形式的声明为依赖列表。

    非法条目（空名/非法字符/路径不合法/重名）跳过并记录告警，保证
    配置错误不会阻止服务启动；输入文本不进入异常或对外信息。
    """
    if not raw:
        return []
    specs: list[DependencySpec] = []
    seen: set[str] = set()
    for index, chunk in enumerate(raw.split(",")):
        entry = chunk.strip()
        if not entry:
            continue
        name, sep, path = entry.partition(":")
        name = name.strip()
        path = path.strip()
        if not sep or not _valid_name(name):
            logger.warning(
                "ignoring malformed aggregate dependency entry",
                extra={"component": "registry", "entry_index": index, "reason": "bad_name"},
            )
            continue
        if not _valid_path(path):
            logger.warning(
                "ignoring malformed aggregate dependency entry",
                extra={"component": "registry", "dependency": name, "reason": "bad_path"},
            )
            continue
        if name in seen:
            logger.warning(
                "ignoring duplicate aggregate dependency entry",
                extra={"component": "registry", "dependency": name},
            )
            continue
        seen.add(name)
        specs.append(DependencySpec(name=name, path=path))
    return specs


class DependencyRegistry:
    """已声明依赖的只读白名单，供聚合路由按名选择扇出集合。"""

    def __init__(self, specs: Iterable[DependencySpec]) -> None:
        # 保留声明顺序，同时建名称索引；构造输入同样去重/校验
        self._specs: list[DependencySpec] = []
        self._index: dict[str, DependencySpec] = {}
        for spec in specs:
            if not _valid_name(spec.name) or not _valid_path(spec.path):
                continue
            if spec.name in self._index:
                continue
            self._specs.append(spec)
            self._index[spec.name] = spec

    @classmethod
    def from_config(cls, raw: str | None) -> DependencyRegistry:
        return cls(parse_dependency_config(raw))

    @classmethod
    def from_environment(cls) -> DependencyRegistry:
        """从环境变量构建；未声明时回落到内置的多依赖默认集合。"""
        raw = os.environ.get(DEPENDENCIES_ENV)
        if raw is None:
            raw = DEFAULT_DEPENDENCY_CONFIG
        return cls.from_config(raw)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(spec.name for spec in self._specs)

    @property
    def all(self) -> tuple[DependencySpec, ...]:
        return tuple(self._specs)

    def select(self, requested: Sequence[str] | None) -> list[DependencySpec]:
        """按名称选择扇出子集。

        ``None``/空 -> 全部依赖；名称去重；出现任何未知名称即抛
        :class:`UnknownDependencySelectionError`（携带可用名单）。
        返回顺序固定为注册表声明顺序。
        """
        if not requested:
            return list(self._specs)
        wanted: set[str] = set()
        unknown: list[str] = []
        for raw_name in requested:
            name = raw_name.strip()
            if not name:
                continue
            if name not in self._index:
                if name not in unknown:
                    unknown.append(name)
                continue
            wanted.add(name)
        if unknown:
            raise UnknownDependencySelectionError(unknown, self.names)
        # 固定按注册表声明顺序返回，与选择参数的书写顺序无关
        return [spec for spec in self._specs if spec.name in wanted]
