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

### 2.2.1 聚合场景的逐依赖指标

聚合请求（`GET /aggregate`，见第 5 节）在整体请求维度之外，按“哪个
依赖 + 结果”另记一组指标：

* `dependency_requests_total{dependency,result}`：逐依赖调用计数；
* `dependency_latency_ms{dependency,result}`：逐依赖延迟聚合
  （`count / sum_ms / min_ms / max_ms / avg_ms`）；
* `dependency` 只允许取应用配置的依赖白名单名称
  （`observability/app.py` 的 `AGGREGATE_DEPENDENCY_PATHS` 键），
  `result` 与整体请求共用同一套封闭枚举；
* 请求标识、具体参数值、真实路径等高基数信息**绝不**进入标签。

### 2.3 超出基数上限时的降级行为

`route` 的不同取值数有明确上限（`MAX_ROUTE_CARDINALITY = 50`，含两个
占位标签）。达到上限后：

1. **不再创建新的标签序列**，超出的样本合并到固定占位
   `route=__overflow__`；
2. 快照中 `cardinality.degraded = true`，并累计
   `cardinality.overflow_samples`；
3. 写一条结构化告警日志；
4. **样本不被丢弃、计数与延迟照常累计，请求也不会因此失败。**

`dependency` 标签采用**同一套降级方式**：不同取值数上限为
`MAX_DEPENDENCY_CARDINALITY = 20`，超限后新依赖合并进
`dependency=__overflow__`，快照中 `cardinality.dependency_degraded = true`
并累计 `cardinality.dependency_overflow_samples`；样本不丢、请求不失败。

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

## 5. 聚合查询（多依赖并发）

`GET /aggregate` 面向“同时从多个下游依赖拉数据、拿到（或到点收口）后
合并为一个响应”的场景。实现位于 `observability/aggregate.py`，路由装配
见 `observability/app.py`。

### 5.1 请求形态与依赖白名单

* 可选查询参数 `deps=items,pricing` 指定本次要打的依赖子集；缺省为全部
  已配置依赖；
* 依赖名只能来自应用配置的固定白名单（`AGGREGATE_DEPENDENCY_PATHS`），
  白名单外的名称一律 `400`（固定文案，不回显输入）——这也是依赖标签
  基数有界的来源；
* 每个依赖一个 asyncio 任务并发调用，任务拷贝当前 `Context`，因此每个
  下游请求自动携带**本次请求自己的** `X-Request-ID`，与对外回传的标识
  一致；并发请求之间由 contextvars 隔离，不会串号。

### 5.2 逐依赖归因与整体判定

每个依赖独立沿用第 3 节的错误分类（判定只依据异常类型与响应状态/形态，
不读任何错误文案），结果互不影响、不会混装。响应体中逐依赖条目按名称
排序（与完成顺序无关）：

```json
{
  "status": "partial",
  "dependencies": [
    {"name": "inventory", "status": "ok", "category": null, "data": {"item": "..."}},
    {"name": "items", "status": "ok", "category": null, "data": {"item": "..."}},
    {"name": "pricing", "status": "failed", "category": "downstream_status", "data": null}
  ],
  "trace_id": "..."
}
```

整体结论的判定规则固定，只依赖各依赖的**结果集合**，与依赖返回快慢、
完成顺序无关：

| 结果集合 | `status` | HTTP 状态 | 说明 |
| --- | --- | --- | --- |
| 全部成功 | `ok` | `200` | 各依赖数据按名称合并返回 |
| 有成功也有失败 | `partial` | `200` | 失败依赖带 `category`，成功依赖带 `data` |
| 全部失败 | `failed` | 由整体类别映射（同第 3 节） | 另含 `error`/`category` 字段 |

全部失败时的整体类别按**固定优先级**从各失败类别中选取（
`downstream_timeout` > `downstream_connection` > `downstream_status` >
`downstream_invalid_response` > `internal_error`），同样是确定性的、不
依赖错误文案。对外响应只含稳定类别与成功依赖的数据，不泄露地址、堆栈、
上游原文等底层细节。

### 5.3 总时长受控

整次聚合有总时长上限（环境变量 `AGGREGATE_TOTAL_TIMEOUT`，默认 3.0 秒）。
到点仍未完成的依赖被取消并按 `downstream_timeout` 记入其结果，整次请求
照常收口返回——个别依赖不返回不会拖死整次请求。

### 5.4 指标与兜底收尾

* 整体请求照常计入 `requests_total{route="/aggregate",result,trace}`：
  `ok`/`partial` 记 `success`，全部失败按整体类别记账；
* 每个依赖的成败与延迟计入第 2.2.1 节的逐依赖指标；
* 无论聚合在依赖调用、结果合并还是其它未预期阶段结束，追踪标识在
  响应头、结构化日志与指标结果维度上都对得上：未预期异常由
  `ObservabilityMiddleware` 的兜底收尾直接生成固定结构的 500 错误体
  （Starlette 的 `ServerErrorMiddleware` 在用户中间件之外，若不兜底，
  其响应会绕过追踪头注入），内部串联用的 `X-Error-Category` 头在任何
  路径下都不会透出。

## 6. 本地验证

```bash
# 安装依赖（含 pytest / pytest-asyncio）
uv sync

# 运行全部测试
uv run python -m pytest

# 仅按主题运行
uv run python -m pytest tests/test_trace.py      # 追踪标识与并发隔离
uv run python -m pytest tests/test_metrics.py    # 指标口径与基数降级
uv run python -m pytest tests/test_errors.py     # 下游异常分类与脱敏
uv run python -m pytest tests/test_logging.py    # 结构化日志关联
uv run python -m pytest tests/test_aggregate.py  # 聚合：并发/归因/总时长/逐依赖指标/兜底

# 启动服务
uv run uvicorn main:app --reload

# 手动验证
curl -i http://127.0.0.1:8000/                                   # 既有行为不变
curl -i -H 'X-Request-ID: my-trace-1234' http://127.0.0.1:8000/ # upstream
curl -i -H 'X-Request-ID: bad!!' http://127.0.0.1:8000/         # rejected
curl -i -H 'X-Request-ID: my-trace-1234' \
     'http://127.0.0.1:8000/aggregate?deps=items,pricing'       # 聚合查询
curl -s http://127.0.0.1:8000/metrics | python -m json.tool     # 指标快照
```

下游地址与超时通过环境变量配置：`DOWNSTREAM_BASE_URL`、
`DOWNSTREAM_TIMEOUT`（秒，默认 2.0）；聚合总时长上限：
`AGGREGATE_TOTAL_TIMEOUT`（秒，默认 3.0）。

测试中通过 `app.state.downstream_client` 注入基于 `httpx.MockTransport`
的替身来确定性地模拟超时、连接失败、非成功状态与非法内容；各用例都会
打印输入场景与判定依据（`[input]` / `[judge]`）。
