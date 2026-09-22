# 可观测性约定

本服务在请求入口统一确定**追踪标识**、采集**指标**、输出**结构化日志**，并把下游依赖的失败归一为**稳定的错误类别**。所有能力由 `obs/` 包提供，通过纯 ASGI 中间件 `obs.middleware.ObservabilityMiddleware` 在边界处生效。

## 1. 追踪标识（Trace ID）

### 1.1 格式与来源

| 来源 `X-Trace-Source` | 触发条件 | 服务行为 |
| --- | --- | --- |
| `upstream` | 请求头 `X-Trace-Id` 存在且合法 | 原样沿用，并在响应头中原样回显 |
| `generated` | 请求头**完全缺失** | 服务生成一个 32 位小写十六进制 UUID（无连字符），响应头回显 |
| `invalid` | 请求头存在但为空、纯空白或不合法 | **在进入应用前直接返回 `400`**，响应体为 `{"error":{"code":"invalid_trace_id",...}}`；响应头 `X-Trace-Source: invalid`；不回显非法值 |

- 合法格式（唯一标准）：正则 `^[0-9a-f]{32}$`（32 个小写十六进制字符）。大写十六进制、超长/过短、含分隔符或其他字符均为非法。
- “缺失生成”与“非法拒绝”是两条确定性、对外可区分的路径：前者请求正常处理（`2xx`，source 为 `generated`），后者一定是 `400` 且 source 为 `invalid`。
- 非法请求在服务端仍会记录一条 warning 日志，使用的是服务侧临时生成的标识，便于排查；上游提供的非法字符串不会被记录为 trace id，也不会出现在响应中。

### 1.2 传播与并发隔离

- 标识只在入口（中间件）解析一次，随后写入 `contextvars.ContextVar`（见 `obs/tracing.py`）。
- 同一请求链路内的所有环节——同步处理函数、`async def` 处理函数、下游调用、结构化日志——都通过 `current_trace_id()` 读取同一个标识，无需层层传参。
- `asyncio` 为每个请求任务复制独立上下文；anyio 的线程池桥接同样会把上下文复制进工作线程。因此：
  - 并发请求之间标识不会串号（测试见 `tests/test_tracing.py::test_concurrent_requests_do_not_mix_ids` 与 `tests/test_async_concurrency.py` 的 40 路交错协程测试）；
  - 同一协程内多次 `await` 让出事件循环后标识保持不变。
- 若自行派生后台线程/任务，请使用 `contextvars.copy_context()` 或在任务内重新 `bind_trace_id()`，不要裸起线程依赖隐式继承。
- 响应头始终携带 `X-Trace-Id` 与 `X-Trace-Source`，包括 4xx/5xx 错误响应（非法标识请求除外，见上表）。

### 1.3 下游传播约定

下游 HTTP 调用应携带当前 trace id（建议使用同名 `X-Trace-Id` 请求头），使调用链可跨服务关联。

## 2. 指标（Metrics）

### 2.1 口径

- 每个**进入边界**的 HTTP 请求记录一条样本（包括因 trace id 非法而被 400 拒绝、以及应用抛出未捕获异常的请求），包含请求计数与延迟（秒）。非法标识请求发生在路由匹配之前，其 `route` 标签统一记为 `unmatched`、`outcome` 记为 `client_error`，非法字符串本身不进入标签。
- 延迟统计：`sum`、`avg`、`max`，以及累计直方图桶（Prometheus 风格，`le_0.05`/`le_0.1`/`le_0.25`/`le_0.5`/`le_1.0`/`+Inf`，单位秒）。`+Inf` 桶恒等于该序列请求总数。
- 即使下游失败、应用抛出未捕获异常，请求也**照样计数**（失败计入对应结果维度），不会因为异常而漏统。

### 2.2 标签（维度）——低基数规则

只有三个标签，全部为低基数：

| 标签 | 取值 | 说明 |
| --- | --- | --- |
| `method` | HTTP 方法 | `GET`/`POST`/... |
| `route` | **路由模板** | 如 `/users/{user_id}`；未匹配路由记为 `unmatched` |
| `outcome` | 结果类别 | `success`（<400）、`client_error`（4xx）、`server_error`（5xx，含下游依赖失败经边界映射后的 5xx） |

**禁止进入标签的高基数信息**：具体路径参数/资源 ID（如 `user_id`）、trace id、查询串、客户端 IP、异常消息文本、下游响应体等。这类信息只能进入（有留存策略的）结构化日志，不能成为指标标签。

### 2.3 基数超限的降级行为

收集器最多保留 50 个不同标签序列（`MetricsCollector(max_series=...)` 可配）。当不同标签组合数量达到上限后：

- **不丢弃**任何样本，**不影响**请求（不报错、不增加延迟路径上的异常）；
- 新出现的标签组合被折叠进唯一的兜底序列（`route="other"`，仍保留 method 置空与 outcome 维度）；
- 同时累加 `degraded_records` 计数，可在 `GET /internal/metrics` 的返回中直接观察到降级发生。

`GET /internal/metrics` 返回所有序列的 JSON 快照（仅用于内部/本地验证，不应暴露到公网）。

## 3. 错误分类（下游依赖）

`obs/errors.py` 把下游失败映射为**稳定、互斥**的类别。分类只依据**异常类型**与 **HTTP 状态码**，不读取、不匹配任何异常消息文本，因此不受库版本升级或语言环境影响。

| 类别 `category` | 含义 | 典型触发（按类型判定） | 对外 HTTP 状态 |
| --- | --- | --- | --- |
| `timeout` | 已建立调用后的读/写超时 | `httpx.ReadTimeout`/`WriteTimeout`、内置 `TimeoutError` | `504` |
| `connection_error` | 连不上/拿不到连接 | `ConnectError`、`ConnectTimeout`、`PoolTimeout`、`NetworkError` | `502` |
| `upstream_5xx` | 下游返回 5xx | `HTTPStatusError` 且状态码 ≥ 500 | `502` |
| `upstream_4xx` | 下游返回 4xx | `HTTPStatusError` 且 400 ≤ 状态码 < 500 | `502` |
| `bad_response` | 响应内容非法 | 响应体无法解码/非 JSON、JSON 不是对象、缺少非空字符串字段 `quote` | `502` |
| `unknown` | 未预期的其他错误 | 不属于以上任何类型的异常 | `500` |

说明：

- 连接阶段的超时（`ConnectTimeout`/`PoolTimeout`）归入 `connection_error` 而非 `timeout`，因为其语义是“无法获得连接”。该映射同样只基于异常类型。
- 对外错误响应固定为统一结构：`{"error":{"code":"dependency_failure","category":"<类别>","message":"downstream dependency request failed"}}`。**不包含**下游主机名、状态原因、响应体、堆栈等底层细节（这些只出现在服务端结构化日志中，含 `error_category` 字段，异常时另有 `exc_info`）。
- 未被业务捕获的其他异常由中间件兜底为 `500 {"error":{"code":"internal_error",...}}`，同样不透出异常细节。

## 4. 结构化日志

- 所有应用日志以单行 JSON 输出到 stderr，字段至少包含：`level`、`logger`、`message`、`trace_id`；通过 `extra={...}` 传入的结构化字段（如 `route`、`status_code`、`duration_ms`、`error_category`、`user_id`）会平铺进 JSON。
- 每条日志自动注入当前上下文的 trace id：请求处理期间所有日志行（含异常日志、下游失败日志、访问完成日志）都带有本请求的标识；请求上下文之外的日志 `trace_id` 为 `null`。

## 5. HTTP 接口

| 方法/路径 | 说明 |
| --- | --- |
| `GET /` | 既有接口，行为保持不变：`200 {"Hello":"World"}` |
| `GET /users/{user_id}` | 示例：路径参数参与业务与日志，但指标按模板 `/users/{user_id}` 聚合 |
| `GET /quote` | 调用下游依赖；成功返回 `{"quote":..., "trace_id":...}`，失败按第 3 节返回分类错误 |
| `GET /internal/metrics` | 指标 JSON 快照（含 `series` 与 `degraded_records`） |
| `GET /async-check/{id}` | 可观测性自测端点：在多次 `await` 交错中校验上下文标识稳定 |

## 6. 本地验证

```bash
# 安装（需要 Python >= 3.14；开发依赖含 pytest）
uv sync

# 运行全部测试（69 个用例），测试中会打印 [输入]/[判定依据] 说明
uv run pytest -q -s

# 启动服务
uv run uvicorn main:app --reload
```

手工验证（另一个终端）：

```bash
# 1) 上游合法标识：回显 source=upstream
curl -s -D - http://127.0.0.1:8000/quote \
  -H 'X-Trace-Id: 11111111111111111111111111111111'

# 2) 缺失标识：自动生成，source=generated
curl -s -D - http://127.0.0.1:8000/

# 3) 非法标识：400 + source=invalid，不回显非法值
curl -s -D - http://127.0.0.1:8000/ -H 'X-Trace-Id: nope!!'

# 4) 指标：观察 route 模板标签与 outcome 维度
curl -s http://127.0.0.1:8000/internal/metrics
```

测试覆盖对应关系：

| 关注点 | 测试文件 |
| --- | --- |
| 标识解析/缺失/非法的确定性区分 | `tests/test_tracing.py::TestResolve` |
| 异步/线程并发不串号、同一链路标识稳定 | `tests/test_tracing.py`、`tests/test_async_concurrency.py` |
| 指标口径、模板标签、基数超限降级不丢弃 | `tests/test_metrics.py` |
| 下游四类+异常的稳定分类、不依赖消息文本 | `tests/test_errors.py` |
| 对外错误响应不泄露底层细节且类别可区分 | `tests/test_http_boundary.py` |
| 结构化日志带 trace id、异常细节只在服务端 | `tests/test_logging.py` |
