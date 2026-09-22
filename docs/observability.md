# 可观测性约定（追踪标识 / 指标 / 错误分类）

本文说明本服务在追踪标识传播、指标口径与标签基数、下游错误分类上的确定
性约定，以及本地验证方法。实现位于 `observability/` 包，ASGI 入口装配见
`observability/app.py`，`main.py` 仅重新导出 `app`，启动方式不变
（`uvicorn main:app`）。

## 1. 追踪标识（Trace ID）

### 1.1 入口确定规则

入口（ASGI 中间件 `ObservabilityMiddleware`）只读取 **一个** 请求头：

| 请求头 | 说明 |
| --- | --- |
| `X-Request-ID` | 上游传入的追踪标识 |

合法性判定是确定的、与外部系统无关：

* 字符集：ASCII 字母、数字与 `-`、`_`、`.`；
* 长度：`8..128`；
* 不满足即判为非法。

处理结果三选一，且对调用方可区分：

| 上游头 | 处理 | 最终标识 | 响应头 `X-Trace-Source` |
| --- | --- | --- | --- |
| 缺失或为空 | 服务端生成 UUID4（32 位十六进制） | 新标识 | `generated` |
| 合法 | 原样透传，不重写 | 上游值 | `upstream` |
| 非法 | **丢弃** 上游值并重新生成 UUID4 | 新标识 | `rejected` |

无论哪种情况，响应都会回传：

* `X-Request-ID`：本次请求最终生效的标识；
* `X-Trace-Source`：`upstream` / `generated` / `rejected`。

非法标识不会导致请求失败：请求仍被正常处理，只是标识被替换，并通过响应头
与指标的 `trace=rejected` 维度对外可区分。

### 1.2 沿调用链传播与并发隔离

* 标识在入口写入 Python `contextvars.ContextVar`，随后的路由处理、下游
  调用、日志全部读取同一上下文值；
* 下游 HTTP 请求自动携带同一个 `X-Request-ID`（见
  `DownstreamClient.get_json`）；
* 每条结构化日志带 `trace_id` 字段；
* asyncio 为每个任务拷贝 `Context`，因此并发请求各自持有独立标识，互不
  串号；中间件在 `finally` 中 reset 绑定，请求结束后上下文不残留。
* 选择纯 ASGI 中间件而非 `BaseHTTPMiddleware`，是为了让中间件与端点处于
  同一任务上下文，保证 `contextvars` 绑定对端点发起的下游调用可见。

## 2. 指标（Metrics）

指标保存在进程内注册表（`observability/metrics.py`），通过
`GET /metrics` 导出 JSON 快照。

### 2.1 指标口径

* `requests_total{route,result,trace}`：请求计数；
* `request_latency_ms{route,result,trace}`：延迟（毫秒）聚合，含
  `count / sum_ms / min_ms / max_ms / avg_ms`；
* 延迟在中间件内围绕整个应用调用以 `perf_counter` 测量。

`result` 取值封闭：

| 值 | 含义 |
| --- | --- |
| `success` | 2xx |
| `client_error` | 4xx |
| `downstream_timeout` | 下游超时 |
| `downstream_connection` | 下游连接失败 |
| `downstream_status` | 下游非成功状态 |
| `downstream_invalid` | 下游 2xx 但响应内容非法 |
| `internal_error` | 其余 5xx / 未处理异常 |

`trace` 取值封闭：`upstream` / `generated` / `rejected`，表示标识来源。

### 2.2 标签基数规则（防止高基数）

* `route` 永远使用 **路由模板**（如 `/items/{item_id}`），不使用原始路径，
  因此 path 参数值、查询串、主机名等不会进入标签；
* 未匹配任何路由的请求统一记为 `route=__unmatched__`；
* `result`、`trace` 均为取值固定的枚举；
* 追踪标识的**值本身**不会出现在任何指标标签中。

### 2.3 超出基数上限时的降级行为

`route` 的不同取值数有明确上限（`MAX_ROUTE_CARDINALITY = 50`，含两个
占位标签）。达到上限后：

1. **不再创建新的标签序列**，超出的样本合并到固定占位
   `route=__overflow__`；
2. 快照中 `cardinality.degraded = true`，并累计
   `cardinality.overflow_samples`；
3. 写一条结构化告警日志；
4. **样本不被丢弃、计数与延迟照常累计，请求也不会因此失败。**

## 3. 下游错误分类

下游调用经 `DownstreamClient`（httpx 异步客户端）。分类只依据 **异常
类型** 与 **响应的状态码/内容形态**，不读取、不匹配任何异常消息文本，
因此底层库升级措辞不会改变归因。

| 触发条件（依据） | 错误类别 `category` | 对外 HTTP 状态 |
| --- | --- | --- |
| `httpx.TimeoutException`（连接/读/连接池超时） | `downstream_timeout` | `504` |
| 其余 `httpx.TransportError` / `RequestError`（DNS、拒绝连接、重置、协议错误等） | `downstream_connection` | `502` |
| 下游返回非 2xx 状态码 | `downstream_status` | `502` |
| 2xx 但 body 非 JSON | `downstream_invalid_response`（reason=`not_json`） | `502` |
| 2xx 且是 JSON 但不是对象 | `downstream_invalid_response`（reason=`not_object`） | `502` |
| JSON 对象缺少非空字符串字段 `item` | `downstream_invalid_response`（reason=`missing_item`） | `502` |
| 服务自身未预期异常 | `internal_error` | `500` |

### 3.1 对外响应形态

错误响应体固定为三个字段，不含主机、端口、URL、上游原文、堆栈等底层
细节：

```json
{
  "error": "downstream request timed out",
  "category": "downstream_timeout",
  "trace_id": "..."
}
```

* `error` 是面向调用方的稳定短语（见 `CATEGORY_MESSAGE`）；
* `category` 是上表中的稳定枚举值，调用方应据此编程；
* `trace_id` 与日志、下游调用链上的标识一致，可用于对账；
* 底层细节（含上游状态码数值、异常堆栈）只写入服务端结构化日志；
* 仅供内部使用的 `X-Error-Category` 响应头在出口被中间件剥离，不会透出。

## 4. 结构化日志

* 所有日志为单行 JSON，固定字段：
  `timestamp / level / logger / message / trace_id`，`extra` 字段平铺；
* 未绑定标识时 `trace_id` 为 `-`；
* 异常以 `exception` 字段记录服务端堆栈，仅入日志。

## 5. 本地验证

```bash
# 安装依赖（含 pytest / pytest-asyncio）
uv sync

# 运行全部测试
uv run python -m pytest

# 仅按主题运行
uv run python -m pytest tests/test_trace.py     # 追踪标识与并发隔离
uv run python -m pytest tests/test_metrics.py   # 指标口径与基数降级
uv run python -m pytest tests/test_errors.py    # 下游异常分类与脱敏
uv run python -m pytest tests/test_logging.py   # 结构化日志关联

# 启动服务
uv run uvicorn main:app --reload

# 手动验证
curl -i http://127.0.0.1:8000/                                   # 既有行为不变
curl -i -H 'X-Request-ID: my-trace-1234' http://127.0.0.1:8000/ # upstream
curl -i -H 'X-Request-ID: bad!!' http://127.0.0.1:8000/         # rejected
curl -s http://127.0.0.1:8000/metrics | python -m json.tool     # 指标快照
```

下游地址与超时通过环境变量配置：`DOWNSTREAM_BASE_URL`、
`DOWNSTREAM_TIMEOUT`（秒，默认 2.0）。

测试中通过 `app.state.downstream_client` 注入基于 `httpx.MockTransport`
的替身来确定性地模拟超时、连接失败、非成功状态与非法内容；各用例都会
打印输入场景与判定依据（`[input]` / `[judge]`）。
