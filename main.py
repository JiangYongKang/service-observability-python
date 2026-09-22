"""服务入口：保持 `uvicorn main:app` 的既有启动方式不变。"""

from observability.app import app

__all__ = ["app"]
