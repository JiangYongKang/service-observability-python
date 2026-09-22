"""结构化 JSON 日志，并为每条记录注入当前请求的追踪标识。

* 所有日志渲染为单行 JSON，字段固定；
* ``TraceIdFilter`` 在 emit 时从 contextvars 读取当前标识，因此同一请求
  各环节（含异步任务）输出的日志携带同一标识，并发请求互不串号；
* 下游底层异常的堆栈只进入日志，不进入对外响应。
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from observability.context import current_trace_id

_CONFIGURED = False


class TraceIdFilter(logging.Filter):
    """为日志记录补充 trace_id 字段（取自 contextvars，天然按任务隔离）。"""

    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = current_trace_id() or "-"
        return True


class JsonFormatter(logging.Formatter):
    """将日志记录渲染为单行 JSON，异常详情仅入日志不外泄。"""

    _RESERVED = {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "taskName", "trace_id",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload = self._build_payload(record)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack_info"] = self.formatStack(record.stack_info)
        return json.dumps(payload, ensure_ascii=False, default=str)

    def _build_payload(self, record: logging.LogRecord) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "trace_id": getattr(record, "trace_id", "-"),
        }
        for key, value in record.__dict__.items():
            if key not in self._RESERVED and not key.startswith("_"):
                payload[key] = value
        return payload


def configure_logging(level: int = logging.INFO) -> None:
    """幂等配置根 logger：JSON 输出 + trace_id 注入。"""
    global _CONFIGURED
    if _CONFIGURED:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(TraceIdFilter())
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    # 框架访问日志同样走 JSON 结构
    for noisy in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(noisy).handlers.clear()
        logging.getLogger(noisy).parent = root
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """获取按统一约定配置的 logger。"""
    return logging.getLogger(name)
