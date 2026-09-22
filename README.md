# service-observability-python

在 FastAPI 服务上补齐的一套可观测性能力：请求级追踪标识传播、低基数指标与结构化日志，以及下游依赖失败的稳定错误分类。

## 快速开始

```bash
uv sync                       # 安装依赖（Python >= 3.14，dev 依赖含 pytest）
uv run uvicorn main:app       # 启动服务 http://127.0.0.1:8000
uv run pytest -q -s           # 运行测试（带 [输入]/[判定依据] 输出）
```

## 能力概览

- **追踪标识**：入口解析 `X-Trace-Id`，合法则沿用（`X-Trace-Source: upstream`），缺失则生成（`generated`），存在但非法则在进入应用前返回 `400`（`invalid`）；标识经 `contextvars` 贯穿同步/异步链路，并发请求互不串号。
- **指标**：按 `method` + 路由模板 + 结果类别统计请求量与延迟直方图；高基数信息（资源 ID、trace id 等）不进入标签；标签序列超限时降级到兜底序列并计数，不丢样本、不影响请求。见 `GET /internal/metrics`。
- **错误分类**：下游超时 / 连接失败 / 4xx / 5xx / 响应内容非法 / 未知错误映射为稳定类别，分类只依据异常类型与状态码；对外响应不透出底层异常细节。
- **结构化日志**：单行 JSON 到 stderr，自动注入当前请求 trace id。

详细约定（传播规则、标签基数规则、分类含义、本地验证）见 [`docs/observability.md`](docs/observability.md)。

## 目录

```
obs/
  tracing.py        # trace id 解析、校验、上下文绑定
  metrics.py        # 低基数指标收集器（含基数降级）
  logging_setup.py  # JSON 日志与 trace 上下文过滤器
  errors.py         # 下游错误类别与分类规则
  client.py         # 下游依赖客户端（失败归一为 DependencyError）
  middleware.py     # ASGI 边界中间件（标识/计时/指标/访问日志/兜底500）
  app.py            # 应用组装与路由
tests/              # 并发、指标、错误链路、日志测试
main.py             # 入口（/ 行为保持 {"Hello":"World"} 不变）
```
