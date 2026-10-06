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
| `partial_failure` | 聚合请求：部分依赖失败 |
| `aggregate_failed` | 聚合请求：全部依赖失败 |

`trace` 取值封闭：`upstream` / `generated` / `rejected`，表示标识来源。

### 2.1.1 逐依赖指标（聚合请求）

聚合请求（`GET /aggregate`）除上述整体指标外，还按“具体哪个依赖 + 结果”
记录：

* `dependency_requests_total{dependency,result}`：依赖调用计数；
* `dependency_latency_ms{dependency,result}`：依赖调用延迟（毫秒）聚合，
  含 `count / sum_ms / min_ms / max_ms / avg_ms`。

`dependency` 只取依赖声明中的**稳定名称**（配置白名单），`result` 复用上
述封闭枚举；请求标识、参数值、真实路径等高基数信息绝不进入标签。

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

依赖标签同理：`dependency` 的不同取值数上限为
`MAX_DEPENDENCY_CARDINALITY = 20`（含占位）。超限后新依赖合并到
`dependency=__overflow__`，快照中 `dependency_cardinality.degraded = true`
并累计 `dependency_cardinality.overflow_samples`；样本不丢、请求不失败。

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

## 4. 聚合查询（`GET /aggregate`）

聚合入口把一次请求扇出为对多个下游依赖的并发调用，全部收口（或到达总
时长上限）后合并为单一响应。实现位于 `observability/aggregation.py`
（`AggregationEngine`），路由装配见 `observability/app.py`。

### 4.1 追踪标识的传递

* 扇出的每个依赖调用都复用第 1 节的机制：经 `contextvars` 读取本次请求
  的标识并以 `X-Request-ID` 透传，因此**每个依赖收到的标识与本次请求
  对外回传的 `X-Request-ID` 完全一致**；
* asyncio 为每个任务拷贝 `Context`，多个聚合请求并发时各自标识互不串号；
* 无论本次聚合在依赖调用、结果合并还是其它未预料阶段结束，中间件都在
  上下文仍绑定标识时完成收尾：响应头 `X-Request-ID`、结构化日志的
  `trace_id` 与指标的 `trace` 维度三者一致；内部串联用的
  `X-Error-Category` 头一律在出口剥离，不对外透出。

### 4.2 逐依赖归因与整体结论

每个依赖独立沿用第 3 节的错误分类（只依据异常类型与响应状态/形态，不
依赖任何错误文案）。响应体按**依赖声明顺序**给出逐依赖结果，与依赖返
回快慢、完成顺序无关：

```json
{
  "status": "partial",
  "trace_id": "agg-trace-0001",
  "dependencies": {
    "alpha": {"status": "ok", "data": {"item": "widget"}},
    "beta": {"status": "error", "category": "downstream_status"}
  }
}
```

整体结论确定性三选一，只取决于各依赖的成败计数：

| 结论 `status` | 判定 | 对外 HTTP 状态 | 指标 `result` |
| --- | --- | --- | --- |
| `ok` | 全部依赖成功 | `200` | `success` |
| `partial` | 部分成功、部分失败 | `200` | `partial_failure` |
| `failed` | 全部失败 | `502` | `aggregate_failed` |

失败依赖的条目只含稳定 `category`（第 3 节枚举），不含上游状态码、地
址、堆栈、上游原文等底层细节；这些只写入服务端结构化日志。

### 4.3 总时长收口

整次聚合受 `AGGREGATE_TOTAL_TIMEOUT`（秒，默认 `3.0`）限制：到点仍未
完成的依赖被取消并记为 `downstream_timeout`，其余已完成的依赖结果照常
合并——**单个依赖不返回不会拖死整次请求**。

### 4.4 打哪些下游：依赖注册表与使用方选择

可聚合的下游集合由**依赖注册表**（`observability/registry.py` 的
`DependencyRegistry`）声明，是一张“稳定名称 → 相对路径”的白名单：

* **部署配置**：通过环境变量 `AGGREGATE_DEPENDENCIES` 声明，形如

  ```
  AGGREGATE_DEPENDENCIES="items:/items/aggregate,stock:/stock/summary,pricing:/pricing/summary"
  ```

  名称允许 `[A-Za-z0-9_.-]{1,64}`；路径必须是以单个 `/` 开头、长度不超过
  200、不含空白与 `://` 的相对路径。非法条目（空名/非法字符/非法路径）与
  重名条目会被跳过并写一条结构化告警，**不会阻止服务启动**。未设置该环境
  变量时使用内置的多依赖默认集合
  （`DEFAULT_DEPENDENCY_CONFIG`，默认即 `items/stock/pricing` 三个下游），
  因此正常调用 `GET /aggregate` 一次就会**并发扇出多个下游**，无需在测试
  里手工构造依赖。
* **使用方选择**：调用方可在请求中按名选择扇出子集：
  * 不带 `deps`：扇出全部已声明依赖；
  * `?deps=items,pricing`（逗号分隔）或 `?deps=items&deps=pricing`
    （重复参数）：只扇出命中的依赖；重复名称去重。
* 名称是唯一会进入 `deps` 参数与逐依赖指标标签的内容；真实下游路径只在
  服务内部使用，不进标签、不进对外错误信息。
* 返回的逐依赖结果**固定按注册表声明顺序**排列，与 `deps` 的书写顺序、
  依赖返回快慢/完成顺序都无关。

**未知选择**：`deps` 中出现任何未声明名称，请求在扇出前即被确定性拒绝，
不调用任何下游，返回 `400`：

```json
{
  "error": "unknown dependency selection",
  "category": "unknown_dependency",
  "trace_id": "...",
  "available_dependencies": ["items", "pricing", "stock"]
}
```

响应只回显稳定白名单（本身低基数、可对外枚举），**不回显原始输入**，因此
路径穿越等注入串不会被反射，也不会被拼接进真实路径；整体指标记为
`client_error`，且不产生逐依赖样本（扇出未发生）。

## 5. 结构化日志

* 所有日志为单行 JSON，固定字段：
  `timestamp / level / logger / message / trace_id`，`extra` 字段平铺；
* 未绑定标识时 `trace_id` 为 `-`；
* 异常以 `exception` 字段记录服务端堆栈，仅入日志。

## 6. 本地验证

```bash
# 安装依赖（含 pytest / pytest-asyncio）
uv sync

# 运行全部测试
uv run python -m pytest

# 仅按主题运行
uv run python -m pytest tests/test_trace.py                  # 追踪标识与并发隔离
uv run python -m pytest tests/test_metrics.py                # 指标口径与基数降级
uv run python -m pytest tests/test_errors.py                 # 下游异常分类与脱敏
uv run python -m pytest tests/test_logging.py                # 结构化日志关联
uv run python -m pytest tests/test_aggregation.py            # 聚合：并发/归因/收口/逐依赖指标/兜底
uv run python -m pytest tests/test_aggregate_selection.py    # 真实路径多依赖扇出/按名选择/未知选择 400

# 启动服务
uv run uvicorn main:app --reload

# 手动验证（默认声明 items/stock/pricing 三个下游，正常调用即并发扇出）
curl -i http://127.0.0.1:8000/                                              # 既有行为不变
curl -i -H 'X-Request-ID: my-trace-1234' http://127.0.0.1:8000/             # upstream
curl -i -H 'X-Request-ID: bad!!' http://127.0.0.1:8000/                     # rejected
curl -i -H 'X-Request-ID: my-trace-1234' http://127.0.0.1:8000/aggregate    # 扇出全部下游
curl -i 'http://127.0.0.1:8000/aggregate?deps=pricing,items'                # 只扇出选中子集（按声明顺序返回）
curl -i 'http://127.0.0.1:8000/aggregate?deps=nope'                         # 未知选择 -> 400
curl -s http://127.0.0.1:8000/metrics | python -m json.tool                 # 指标快照
```

下游地址与超时通过环境变量配置：`DOWNSTREAM_BASE_URL`、
`DOWNSTREAM_TIMEOUT`（秒，默认 2.0）、`AGGREGATE_TOTAL_TIMEOUT`（聚合总
时长上限，秒，默认 3.0）、`AGGREGATE_DEPENDENCIES`（可聚合下游声明，
`name:/path,name:/path`；未设置时为内置的三依赖默认集合，见 4.4 节）。

运行测试时每个用例会先打印一行 `===== 业务用例 :: <文件>::<用例> =====`
标题，用例体内再打印 `[input]`（场景输入）与 `[judge]`（判定情况），从
单测日志即可看出覆盖了哪些业务、各业务有哪些用例以及每条判定结论。

测试中通过 `app.state.downstream_client` 注入基于 `httpx.MockTransport`
的替身来确定性地模拟超时、连接失败、非成功状态与非法内容；聚合相关测试
通过 `app.state.aggregate_registry`（或 `aggregate_dependencies`）注入
依赖集合。各用例都会打印输入场景与判定依据（`[input]` / `[judge]`）。
