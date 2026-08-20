# 重试与链路语义

本文描述当前代码实际执行的请求分类、策略优先级、重试条件、超时预算和链路头行为。

## 1. 单次出站调用流程

每个请求按以下顺序处理：

1. 解析基础重试策略
2. 确定请求 `Class`
3. 补齐 `Caller`、`Downstream`、`Operation`
4. 应用动态分类策略和第一条匹配规则
5. 应用上游禁止重试和截止时间约束
6. 检查请求体是否可重放
7. 执行首次请求
8. 已满足现有重试条件时，检查当前 downstream 的本地重试预算并执行有限重试
9. 输出最终结果事件和日志

内置 `ManagedRetryConfigProvider` 会在同一个配置读锁内完成分类、Scope、策略和重试预算参数解析。一次请求只使用同一代动态配置。

## 2. 默认策略

- 读请求：最多额外重试一次
- 写请求：默认不自动重试
- 单次尝试默认超时：`2s`
- 初始退避：`100ms`
- 最大退避：`2s`
- 默认状态码：`408`、`429`、`500`、`502`、`503`、`504`

配置可以为具备幂等保障的写请求开启有限重试。组件不会判断业务操作是否真正幂等。

## 3. 可重试错误

满足重试次数、预算、链路约束和 Body 可重放条件后，以下错误可以重试：

- `context.DeadlineExceeded`
- `io.ErrUnexpectedEOF`
- 连接拒绝、连接重置、broken pipe
- DNS timeout 或 DNS temporary error
- `net.Error` 报告的 timeout
- 配置允许的 HTTP 状态码

以下情况不重试：

- `context.Canceled`
- TLS 证书校验失败
- 未进入配置状态码集合的响应
- `429` 未同时满足 `retry_on_statuses` 和 `retry_on_429`
- 上游或 Context 已禁止重试
- 请求体不可重放

HTTP `4xx/5xx` 是否触发重试与辅助函数最终是否返回 Go error 是两个概念。辅助函数不会仅因为最终状态码非 2xx 而返回 error。

## 4. 退避

组件使用指数退避和随机抖动。加入抖动后的等待时间不会超过 `max_backoff`。

响应包含合法 `Retry-After` 时优先使用该值。开始等待前会检查整体截止时间；等待会超过预算时不再重试。

无法表示为 `time.Duration` 的超大秒数会被视为无效值，并回退到本地退避策略。

## 5. 超时预算

整体截止时间取以下值中的最早者：

1. 请求 Context deadline
2. `X-Request-Deadline`
3. 当前策略的 `max_elapsed_time`

每次尝试的 deadline 再取整体截止时间与 `per_attempt_timeout` 中更早者。

该预算约束 HTTP 尝试和退避等待。自定义 Resolver、Provider、Skipper 和 Observer 是同步扩展回调，无法由组件强制中断；回调可能消耗剩余预算，并可能使实际返回时间超过 `max_elapsed_time`，因此必须快速返回。

`X-Request-Deadline` 使用 Unix 毫秒时间戳：

```http
X-Request-Deadline: 1893456000000
```

无效 Header 会被忽略；Gin 入站中间件会记录警告日志。

`X-Request-Deadline` 是绝对时间，各服务节点需要保持可靠的时钟同步。截止时间已到时，调用返回的传输错误为 `context.DeadlineExceeded`；如果已经收到最终 HTTP 响应，则仍由调用方处理该响应。

## 6. 本地重试预算

配置 `retry_budget` 后，每个 `Transport` 按 downstream 维护独立余额。首次请求永远不受预算限制；只有策略、状态码或传输错误、链路约束、Body 可重放性、重试次数和整体截止时间均已允许下一次尝试时，组件才预留一次 `retry_cost`。所有重试原因使用相同成本。

预算不足时：

- 不执行额外尝试，也不等待本次退避
- 保留当前响应或传输错误作为最终结果
- 最终事件的 `RetrySuppressedReason` 和日志字段 `retrySuppressedReason` 为 `budget_exhausted`
- 输出 `HTTPClient retry suppressed` Warn 日志

预留后，如果退避等待被取消，或准备下一次尝试时因 `GetBody` 等原因失败，预留会被归还；下一次 HTTP 尝试真正开始前，预留才会提交。

一次逻辑请求最终满足 `err == nil`、响应非空且 HTTP 状态码 `< 400` 时，恢复一次 `success_increment`。首次成功和重试后成功都恢复，每个逻辑请求最多恢复一次，因此一次重试成功的净消耗仍为 `retry_cost - success_increment`。余额不会超过 `capacity`。

省略 `retry_budget` 或设置 `enabled: false` 时完全绕过预算，保持原有重试行为。预算是单进程、单 `Transport` 的本地保护，不在实例之间共享，也不替代幂等保障、deadline、首次请求限流、熔断或服务端过载保护。

## 7. 链路约束

### 7.1 `X-No-More-Retry`

值为 `true` 时，当前请求以及由其 Context 派生的下游调用都不能自动重试。下游配置不能重新放宽该约束。

组件也会在当前调用的最后一次允许尝试上自动发送 `X-No-More-Retry: true`，避免下游在调用方已经耗尽本地重试额度时再次放大请求。

### 7.2 `X-Request-Deadline`

表示整条链路的绝对截止时间。每一跳可以进一步缩短预算，但不能延长。

### 7.3 `X-Retry-Attempt`

表示当前调用方对当前下游的零基尝试序号。新的下游调用重新从 `0` 开始。

### 7.4 `X-Retry-Reason`

表示当前调用关系中最近一次重试原因。当前值包括：

- `timeout`
- `transport_error`
- `429`
- `5xx`
- `status_<code>`

该 Header 不跨服务继承。

## 8. Gin 传播

`GovernanceMiddleware()` 只将以下入站 Header 导入标准请求 Context：

- `X-No-More-Retry`
- `X-Request-Deadline`

出站 Transport 再根据该 Context 生成下一跳 Header。`X-Retry-Attempt` 和 `X-Retry-Reason` 不会写入传播 Context。

```go
router.Use(ginmiddleware.GovernanceMiddleware())

func handler(c *gin.Context) {
	ctx := c.Request.Context()
	_ = httpclient.Get(ctx, targetURL, &response)
}
```

## 9. 请求结果

每次 Transport 完成出站调用后，组件会产生 `RequestResultEvent`。事件区分：

- 是否发生过重试
- 重试后是否成功
- 最终是否失败
- 是否超时
- 最终 HTTP 状态码和错误原因
- 重试是否因本地预算耗尽而被抑制

调用方可以实现 `RetryObserver` 和 `RetryResultObserver` 接入自己的指标或追踪系统。通过 `SetRetryObserver(...)` 和 `SetRetryResultObserver(...)` 可以分别注册两类观察者；为兼容旧代码，实现了两个接口的 `RetryObserver` 仍可同时接收结果事件。

结果事件是 Transport 级结果：统计从首次尝试到最终响应 Header 或传输错误为止的时间，不包含辅助函数随后执行的响应 Body 解码，也不会把 JSON/Protobuf 解码失败记录为传输失败。

`TimedOut` 在本地/Context deadline、网络超时、HTTP `408` 或 HTTP `504` 时为 `true`。`FinalFailed` 在发生传输错误或最终状态码为 `4xx/5xx` 时为 `true`。

观察者回调是同步调用的，并可能被多个请求并发执行。实现必须并发安全且快速返回；阻塞回调会增加请求耗时，并可能使调用的实际返回时间超过 `max_elapsed_time`。

## 10. Skipper

`SkipperFunc` 命中时会跳过请求分类、动态策略和自动重试，但不会移除链路约束。组件仍会传播 `X-No-More-Retry`、`X-Request-Deadline`，并重置当前调用关系的 Attempt/Reason Header。
