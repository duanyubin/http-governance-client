# 故障排查

## 初始化失败

检查：

- 使用 `SetupWithOptions(...)` 时，`Consul` 为空表示不启用 Consul，不会导致初始化失败
- 直接调用 `SetRetryConfigProviderFromConsul(...)` 等 Consul API 时，地址不能为空
- Consul 地址是否可访问
- 地址是否需要显式添加 `http://` 或 `https://`
- YAML 是否包含未知字段
- duration 是否为合法且大于零的 Go duration
- 未禁用的 rule 是否包含有效 `match` 和 `policy`
- downstream 是否同时包含非空 `name` 和 `hosts`

初始化失败不会安装不完整的动态配置。

## 配置没有热更新

检查：

1. 是否使用 `SetupWithOptions(...)`、`Setup()` 或 `SetRetryConfigProviderFromConsul(...)`
2. 是否只调用了只读一次的 `NewManagedRetryConfigProviderFromConsul(...)`
3. 是否在初始化前将热更新间隔设置为 `<= 0`
4. 当前进程是否又为另一个配置对象启动了后台热更新任务
5. 日志中是否存在 `HTTPClient retry config hot reload failed`

一个进程只有一个包级自动热更新任务。

## 配置更新失败后使用什么策略

新配置会先解析和编译。失败时继续使用上一份有效配置，并在后续监听周期重试加载。

服务级 Key 删除后回退到应用级配置；两个 Key 都不存在时使用内置默认策略。

## 请求未发生重试

依次检查：

- 最终策略的 `max_retries` 是否大于零
- `disable_retry`、`X-No-More-Retry` 或 `WithNoMoreRetry` 是否禁用了重试
- HTTP 状态码是否位于 `retry_on_statuses`
- `retry_on_statuses` 是否只包含 `100..599` 的三位 HTTP 状态码
- `429` 是否同时允许 `retry_on_429`
- Context 或整体预算是否已经耗尽
- 请求 Body 是否存在且 `GetBody == nil`
- 错误是否属于可重试传输错误

## 标准 `http.Request` 未应用治理

使用 `http.NewRequestWithContext(...)` 创建请求并不会自动替换发送请求的 Client。检查请求是否通过以下任一方式发送：

- `httpclient.Do(req, expectedPtr)`
- `httpclient.Instance().Do(req)`
- `httpclient.NewClientWithOptions(...)` 返回的独立 Client

`http.DefaultClient.Do(req)`、`http.Get(...)` 以及其他未使用组件 Transport 的 Client 不会应用动态重试、超时预算或链路头。

## 请求被识别为 external

使用内置 YAML 或 Consul 配置时，只有命中 `internal_hosts` 的主机才会识别为 internal。空列表和未命中主机都会识别为 external。

检查 URL 的 hostname、端口以及 glob 是否与实际请求一致。必要时使用 `WithInternalRead`、`WithInternalWrite` 或 `WithRetryConfigScope` 显式覆盖。

## error 为 nil 但 HTTP 状态失败

辅助函数不会把最终 `4xx/5xx` 自动转换为 Go error。使用 `ResponsePtr.StatusCode` 检查状态码：

```go
var body APIResponse
result := httpclient.ResponsePtr{ExpectedPtr: &body}
err := httpclient.InternalGet(ctx, targetURL, &result, nil)
```

## 链路约束没有传播

Gin 服务需要：

1. 注册 `GovernanceMiddleware()`
2. 调用下游时传递 `c.Request.Context()`
3. 确认网关或代理没有删除 `X-No-More-Retry` 和 `X-Request-Deadline`

`X-Retry-Attempt` 和 `X-Retry-Reason` 本来就不会跨服务继承。

如果不同服务对 `X-Request-Deadline` 的剩余时间判断不一致，请检查节点时钟同步；该 Header 使用绝对 Unix 毫秒时间戳。

## 观察热更新和重试

关注以下日志：

- `HTTPClient retry scheduled`
- `HTTPClient request finished`
- `HTTPClient retry config reloaded`
- `HTTPClient retry config hot reload failed`

需要自定义指标时，实现 `RetryObserver` 和 `RetryResultObserver`，并分别通过 `SetRetryObserver(...)`、`SetRetryResultObserver(...)` 注册。回调必须并发安全且避免阻塞。
