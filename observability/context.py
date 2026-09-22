"""追踪标识的确定、校验与基于 contextvars 的按请求隔离。

约定（详见 docs/observability.md）：

* 入口从 ``X-Request-ID`` 读取上游标识；
* 缺失 → 服务端生成 UUID4，来源 ``generated``；
* 非法（长度不在 8..128 或含白名单外字符）→ 丢弃并重新生成，来源
  ``rejected``；合法则原样透传，来源 ``upstream``；
* 标识通过 ``contextvars`` 绑定当前执行上下文，asyncio 每个任务拷贝
  Context，因此并发/异步请求之间天然隔离、不会串号；
* 同一下游调用通过 ``X-Request-ID`` 头向后透传同一标识。
"""

from __future__ import annotations

import contextvars
import uuid
from dataclasses import dataclass
from enum import Enum

#: 上游传入追踪标识时使用的请求头
TRACE_HEADER = "X-Request-ID"

#: 合法标识允许的字符集（ASCII 字母/数字及三个连接符）
_ALLOWED_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_."
)
_MIN_LEN = 8
_MAX_LEN = 128


class TraceSource(str, Enum):
    """追踪标识的来源，响应头 X-Trace-Source 据此对外可区分。"""

    UPSTREAM = "upstream"  # 上游传入且合法，原样透传
    GENERATED = "generated"  # 上游缺失，服务端新生成
    REJECTED = "rejected"  # 上游传入但非法，已丢弃并新生成


@dataclass(frozen=True, slots=True)
class TraceIdentity:
    """一次请求最终使用的追踪标识及其来源。"""

    trace_id: str
    source: TraceSource


#: 当前执行上下文的追踪标识；每个 asyncio 任务持有独立拷贝
_trace_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "observability_trace_id", default=None
)


def is_valid_trace_id(value: str) -> bool:
    """校验上游标识是否合法：长度与字符集确定，不依赖任何外部规则。"""
    if not isinstance(value, str):
        return False
    if not (_MIN_LEN <= len(value) <= _MAX_LEN):
        return False
    return all(ch in _ALLOWED_CHARS for ch in value)


def generate_trace_id() -> str:
    """生成一个新的追踪标识（UUID4 十六进制，无连字符，长度 32）。"""
    return uuid.uuid4().hex


def resolve_trace_id(raw: str | None) -> TraceIdentity:
    """根据上游头值确定最终标识：缺失则生成，非法则拒绝并生成。"""
    if raw is None or raw == "":
        return TraceIdentity(generate_trace_id(), TraceSource.GENERATED)
    if is_valid_trace_id(raw):
        return TraceIdentity(raw, TraceSource.UPSTREAM)
    return TraceIdentity(generate_trace_id(), TraceSource.REJECTED)


def bind_trace_id(trace_id: str) -> contextvars.Token[str | None]:
    """将标识绑定到当前执行上下文，返回 token 供 reset 使用。"""
    return _trace_var.set(trace_id)


def current_trace_id() -> str | None:
    """读取当前上下文的追踪标识，未绑定时返回 None。"""
    return _trace_var.get()
