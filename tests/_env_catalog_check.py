"""子进程辅助：在 AGGREGATE_DEPENDENCIES 环境变量下启动应用并验证真实扇出。

仅由 test_aggregate_realtime.py 的子进程用例调用，不参与正常用例收集。
"""

from __future__ import annotations

import asyncio
import json

import httpx

from observability.app import DEFAULT_AGGREGATE_DEPENDENCIES, create_app
from observability.downstream import DownstreamClient


async def _handler(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"item": f"w@{request.url.path}"})


async def main() -> None:
    app = create_app()
    app.state.downstream_client = DownstreamClient(
        "http://ds", transport=httpx.MockTransport(_handler)
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t"
    ) as c:
        subset = await c.get("/aggregate?deps=svc_b")
        all_deps = await c.get("/aggregate")
    await app.state.downstream_client.aclose()
    print(
        json.dumps(
            {
                "names": [d.name for d in DEFAULT_AGGREGATE_DEPENDENCIES],
                "paths": [d.path for d in DEFAULT_AGGREGATE_DEPENDENCIES],
                "subset": list(subset.json()["dependencies"]),
                "all": list(all_deps.json()["dependencies"]),
            }
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
