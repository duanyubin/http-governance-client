# 接入与使用

本文说明组件初始化、Gin 接入、请求发送、响应处理和运行时管理。配置字段见[配置参考](config-reference.md)，重试决策顺序见[重试与链路语义](retry-semantics.md)。

## 1. 初始化方式

普通服务只需在启动时初始化一次组件。之后既可以调用 `InternalGet`、`InternalPost` 等辅助函数，也可以使用标准库创建 `http.Request`，再通过组件默认客户端发送；两种方式都会使用同一份重试配置。

### 1.1 显式初始化

推荐在服务启动时调用一次 `SetupWithOptions(...)`：

```go
err := httpclient.SetupWithOptions(httpclient.SetupOptions{
	Name:   "orders-api",
	Consul: "http://127.0.0.1:8500",
})
if err != nil {
	return fmt.Errorf("initialize HTTP governance: %w", err)
}
```

该调用会：

1. 将 `Name` 注册为默认 `Caller`
2. 读取应用级和服务级 Consul 配置
3. 将请求分类和重试策略应用到组件默认客户端
4. 启动配置热更新

Consul KV 路径为：

```text
config/go/application/retry
config/go/{Name}/retry
```

`Env` 仅用于替换 Consul 地址中的 `${profile}`：

```go
err := httpclient.SetupWithOptions(httpclient.SetupOptions{
	Env:    "staging",
	Name:   "orders-api",
	Consul: "http://consul-${profile}.example.com:8500",
})
```

### 1.2 从进程参数初始化

`Setup()` 会读取 `--env`、`--name` 和 `--consul`：

```go
if err := httpclient.Setup(); err != nil {
	return err
}
```

示例参数：

```text
--env staging --name orders-api --consul http://127.0.0.1:8500
```

该入口适用于已经统一使用这些启动参数的服务。通用库代码和测试应优先使用显式参数，避免依赖调用进程的 `os.Args`。

### 1.3 静态 YAML

不使用 Consul 时，可以安装静态配置：

```go
data, err := os.ReadFile("retry.yaml")
if err != nil {
	return err
}
httpclient.SetCallerServiceName("orders-api")
if _, err := httpclient.SetRetryConfigProviderFromYAML(data); err != nil {
	return err
}
```

该方式不会启动后台热更新。

## 2. Gin 接入

每个 Gin 服务注册一次治理中间件：

```go
router.Use(ginmiddleware.GovernanceMiddleware())
```

中间件读取入站的 `X-No-More-Retry` 和 `X-Request-Deadline`，将约束写入 `c.Request.Context()`。

Handler 调用下游时必须继续传递标准请求 Context：

```go
ctx := c.Request.Context()
err := httpclient.InternalGet(ctx, targetURL, &response, nil)
```

以下写法会丢失链路约束：

```go
// 错误：不要替换入站 Context。
err := httpclient.InternalGet(context.Background(), targetURL, &response, nil)
```

不要把 `*gin.Context` 作为 `context.Context` 传给组件。

## 3. 发送请求

### 3.1 快捷辅助函数（可选）

辅助函数用于减少创建 `http.Request`、发送请求、关闭响应 Body 和解析响应的重复代码。它们都使用组件默认客户端；不使用辅助函数也不影响治理功能。

发送 GET 请求：

```go
var response Order
if err := httpclient.InternalGet(ctx, targetURL, &response, nil); err != nil {
	return err
}
```

发送 JSON POST 请求：

```go
if err := httpclient.InternalPost(
	ctx,
	targetURL,
	"application/json",
	requestBody,
	&response,
	nil,
); err != nil {
	return err
}
```

`InternalGet`、`InternalPost`、`InternalPut`、`InternalDelete` 以及对应的 multipart 辅助函数支持在最后一个参数中传入鉴权 Header 设置器；不需要时传 `nil`。不需要设置鉴权 Header 时，也可以使用参数更少的 `Get`、`Post`、`Put`、`Delete`、`Head`、`PostMultipartForm` 和 `PutMultipartForm`。

`Internal` 是为兼容既有 API 保留的命名，只表示该函数支持调用方设置鉴权 Header，不会强制把请求分类为 internal。具体分类规则见“请求分类与策略匹配维度”。

### 3.2 响应状态

辅助函数返回的 error 表示请求创建、鉴权、传输、超时或响应解码失败。HTTP `4xx/5xx` 本身不会自动产生 error。

需要检查状态码时使用 `ResponsePtr`：

```go
var body ErrorResponse
result := httpclient.ResponsePtr{ExpectedPtr: &body}

if err := httpclient.InternalGet(ctx, targetURL, &result, nil); err != nil {
	return err
}
if result.StatusCode != http.StatusOK {
	return fmt.Errorf("unexpected HTTP status %d", result.StatusCode)
}
```

当 `expectedPtr == nil` 时，组件会在有限范围内排空并关闭响应 Body，但不会解析内容。只需要状态码时可以传入空的 `ResponsePtr{}`。

### 3.3 可重放请求体

配置允许写请求重试时，请求体必须可重放。组件的 JSON 和 multipart 辅助函数会创建可重放 Body。

直接构造 `http.Request` 时，应使用标准库能够自动生成 `GetBody` 的 Reader，或自行设置 `GetBody`。如果请求包含 Body 且 `GetBody == nil`，组件会关闭该请求的自动重试。

### 3.4 使用标准 `http.Request`

可以使用标准库的 `http.NewRequestWithContext(...)` 创建请求。治理功能是否生效取决于发送请求时使用的 Client。

需要由组件解析响应时，使用 `httpclient.Do(...)`：

```go
req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
if err != nil {
	return err
}

var body Order
result := httpclient.ResponsePtr{ExpectedPtr: &body}
if err := httpclient.Do(req, &result); err != nil {
	return err
}
if result.StatusCode < 200 || result.StatusCode >= 300 {
	return fmt.Errorf("downstream returned HTTP %d", result.StatusCode)
}
```

需要自行处理标准库 `*http.Response` 时，使用组件默认客户端：

```go
req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
if err != nil {
	return err
}
resp, err := httpclient.Instance().Do(req)
if err != nil {
	return err
}
defer resp.Body.Close()
```

以下调用不会经过组件的治理 Transport，因此不会应用动态重试、超时预算或链路头：

```go
resp, err := http.DefaultClient.Do(req)
```

```go
resp, err := http.Get(targetURL)
```

在 Gin Handler 中，`ctx` 仍应来自 `c.Request.Context()`，以继续传播 `X-No-More-Retry` 和 `X-Request-Deadline`。

## 4. 请求分类与策略匹配维度（Scope）

最终请求类型由 `internal/external` 和 `read/write` 组成：

| 类型 | 含义 |
| --- | --- |
| `internal_read` | 内部读请求 |
| `internal_write` | 内部写请求 |
| `external_read` | 外部读请求 |
| `external_write` | 外部写请求 |

Context 中显式指定的请求分类优先级最高。未显式指定时，使用组件内置的 YAML 或 Consul 配置会按以下规则自动分类：

- 命中 `internal_hosts` → `internal_*`
- 未命中或 `internal_hosts` 为空 → `external_*`
- `GET`、`HEAD`、`OPTIONS`、`TRACE` → `*_read`
- 其他方法 → `*_write`

未使用内置 YAML 或 Consul 配置时，组件优先使用自定义请求分类器；没有自定义分类器时，按请求方法回退为 `internal_read` 或 `internal_write`。

`Caller` 默认来自初始化时的服务名。`Downstream` 和 `Operation` 分别由 `downstreams`、`operations` 自动识别。

仅在自动识别不足时显式覆盖：

```go
ctx = httpclient.WithRetryConfigScope(ctx, httpclient.RetryConfigScope{
	Downstream: "payments",
	Operation:  "queryPayment",
})
```

也可以显式覆盖请求类型：

```go
ctx = httpclient.WithExternalWrite(ctx)
```

## 5. 链路头

| Header | 范围 | 格式 | 行为 |
| --- | --- | --- | --- |
| `X-No-More-Retry` | 整条链路 | `true` | 上游禁止重试后，下游不能重新放宽 |
| `X-Request-Deadline` | 整条链路 | Unix 毫秒时间戳 | 每一跳只能保持或缩短截止时间 |
| `X-Retry-Attempt` | 当前调用关系 | 从 `0` 开始的整数 | 只在当前下游调用的本地重试中递增 |
| `X-Retry-Reason` | 当前调用关系 | `timeout`、`transport_error`、`429`、`5xx` 等 | 不跨服务继承 |

Gin 中间件只导入前两个链路约束。后两个字段由每一跳的出站 Transport 重新计算。

## 6. 热更新生命周期

### 6.1 自定义热更新等待时间

未调用 `SetRetryConfigAutoReloadInterval(...)` 时，等待时间默认为 `30s`：

```go
httpclient.SetRetryConfigAutoReloadInterval(10 * time.Second)
```

该设置必须在初始化之前调用，只影响之后启动的后台热更新任务。传入 `<= 0` 会禁止启动新的后台热更新任务。

### 6.2 配置变更行为

Consul 配置发生创建、更新或删除时，组件会重新读取应用级和服务级配置：

- 服务级 Key 删除后回退到应用级配置
- 两个 Key 都不存在时使用空动态配置和内置默认策略
- 新配置解析或校验失败时保留上一份有效配置，并继续尝试重新加载

### 6.3 停止热更新

测试或需要显式释放资源的程序可以调用：

```go
httpclient.StopRetryConfigAutoReload()
```

## 7. 日志与观测

### 7.1 默认日志

组件通过 `log/slog` 直接输出以下日志：

| 日志 | 级别 | 含义 |
| --- | --- | --- |
| `HTTPClient retry scheduled` | Warn | 已决定执行一次重试 |
| `HTTPClient request finished` | Info | 一次 Transport 出站调用的最终结果 |
| Consul 配置重新加载 | Info / Warn | 热更新成功或失败 |

`HTTPClient request finished` 包含完整请求 URL。请求和响应 Header 只在 Debug 级别的详细日志中输出；Body 日志默认也使用 Debug 级别，可通过 `SetBodyLogLevel(...)` 调整。

Body 日志支持文本、JSON、XML 和 `application/x-protobuf`，默认最多读取并记录 64 KiB。普通响应长度未知时，组件会在该上限内读取并恢复 Body，不会仅因 `Content-Length` 缺失而跳过。为避免阻塞流式传输，未知长度的请求体和 `text/event-stream` 响应不会被预读。需要记录更大的完整 Body 时，应在服务初始化阶段设置上限：

```go
if err := httpclient.SetBodyLogLimit(1024 * 1024); err != nil { // 1 MiB
	return err
}
```

实际 Body 超过上限时不会打印 Body 内容，后续业务读取不受影响。增大上限会增加单次请求的内存和日志量；日志采集器、传输链路和存储端也可能具有独立的事件大小限制。

URL、Header 和 Body 都可能包含查询参数、认证信息、Cookie、Token 或业务数据。生产环境应结合日志等级、采集范围和访问权限完成数据安全评估。

### 7.2 基于最终结果日志统计

`HTTPClient request finished` 使用 Info 级别输出，每条日志表示一次完成的 Transport 出站调用；本地重试不会额外产生最终结果日志。标准 `http.Client` 自动跟随重定向时，每个重定向跳都会产生一条最终结果日志，因此以下指标默认按 Transport 调用量统计：

| 指标 | 统计口径 |
| --- | --- |
| 请求量 | 最终结果日志条数 |
| 超时率 | `timedOut=true` 的日志数 / 请求量 |
| 重试率 | `retried=true` 的日志数 / 请求量 |
| 重试成功率 | `retrySucceeded=true` 的日志数 / `retried=true` 的日志数 |
| 最终失败率 | `finalFailed=true` 的日志数 / 请求量 |
| 额外尝试量 | `retryCount` 求和 |
| 分维度请求量 | 按 `caller/downstream/operation` 分组后统计日志条数 |
| 分维度错误量 | 按 `caller/downstream/operation` 分组后统计 `finalFailed=true` 的日志数 |

`timedOut=true` 包括本地/Context deadline、网络超时、HTTP `408` 和 HTTP `504`。`finalFailed=true` 包括传输错误和最终 HTTP `4xx/5xx`；状态码为 `0` 表示没有收到 HTTP 响应。

为保证聚合维度稳定：

- 初始化时必须设置 `SetupOptions.Name` 或调用 `SetCallerServiceName(...)`。
- 应通过 `downstreams` 和 `operations` 配置稳定的逻辑名称。
- 未配置时，`downstream` 回退为目标 hostname 的第一段，`operation` 回退为 `METHOD + URL.Path`；query 和 fragment 不会进入 operation，但 path 中的资源 ID 仍可能产生高基数。
- 不要使用完整 `url` 作为统计维度，URL 可能包含查询参数和动态资源 ID。
- Info 日志必须开启；命中 `SkipperFunc` 的请求不产生最终结果日志。
- 如需统计包含重定向的 `client.Do` 级调用量，应关闭自动重定向或在调用方按统一请求标识聚合。

### 7.3 可选的 Observer

Observer 用于将重试事件直接接入指标、链路追踪或其他监控系统：

- `RetryObserver`：组件决定执行重试时调用，每次重试产生一个事件
- `RetryResultObserver`：一次出站调用完成后调用一次，接收所有尝试结束后的最终结果

这两个接口都是可选的，不影响重试、超时、日志或配置热更新。仅通过 7.2 中的最终结果日志进行统计时，不需要配置 Observer。需要使用时，由接入者实现接口，并在服务启动时注册一次：

```go
type MetricsObserver struct{}

func (MetricsObserver) ObserveRetry(
	ctx context.Context,
	event httpclient.RetryEvent,
) {
	// 将重试次数、原因、状态码和退避时间写入监控系统。
}

func (MetricsObserver) ObserveRequestResult(
	ctx context.Context,
	event httpclient.RequestResultEvent,
) {
	// 将最终状态、耗时、是否超时和是否重试成功写入监控系统。
}

func initHTTPObserver() {
	observer := MetricsObserver{}
	httpclient.SetRetryObserver(observer)
	httpclient.SetRetryResultObserver(observer)
}
```

`SetRetryObserver(...)` 和 `SetRetryResultObserver(...)` 作用于组件默认客户端。使用独立 Client 时，应通过 `ClientOptions.Observer` 和 `ClientOptions.ResultObserver` 在创建 Client 时传入。

`RetryResultObserver` 的最终事件包含 `caller`、`downstream`、`operation`、`class`、尝试次数、状态码和最终原因等字段。

`RetryResultObserver` 和 `HTTPClient request finished` 使用相同的 Transport 级结果语义，不包含辅助函数之后的 JSON/Protobuf 解码耗时或解码错误。

Observer、Skipper、自定义 Resolver 和 Provider 都可能被多个请求并发调用，必须并发安全。这些扩展点为同步回调，不能被 `max_elapsed_time` 强制中断，可能消耗剩余预算或延迟实际返回，不应执行阻塞操作。

组件不直接提供指标后端或链路追踪 SDK。调用方可通过 Observer 接入自己的监控系统。

## 8. 创建独立 Client（高级）

普通服务无需使用本节。完成初始化后，可以继续使用辅助函数，也可以通过 `httpclient.Do(...)` 或 `httpclient.Instance().Do(...)` 发送标准 `http.Request`。

只有在同一进程中需要单独设置整体超时、重试配置、底层 Transport、重定向规则或 CookieJar 时，才需要通过 `NewClientWithOptions(...)` 创建独立的标准库 `*http.Client`。

独立 Client 必须通过标准库的 `client.Do(req)` 发送请求，不会被包级 `InternalGet`、`InternalPost` 等辅助函数使用：

```go
configData, err := os.ReadFile("retry.yaml")
if err != nil {
	return err
}
provider, err := httpclient.NewManagedRetryConfigProviderFromYAML(configData)
if err != nil {
	return err
}
client := httpclient.NewClientWithOptions(httpclient.ClientOptions{
	ConfigProvider: provider,
	RequestTimeout: 5 * time.Second,
})

req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
if err != nil {
	return err
}
resp, err := client.Do(req)
if err != nil {
	return err
}
defer resp.Body.Close()

if resp.StatusCode < 200 || resp.StatusCode >= 300 {
	return fmt.Errorf("downstream returned HTTP %d", resp.StatusCode)
}
var response Order
if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
	return err
}
```

`RequestTimeout` 是该独立 Client 的整体超时。`ConfigProvider` 只影响该 Client，不会修改组件默认客户端的配置。

`NewManagedRetryConfigProviderFromConsul(...)` 只读取一次配置。如需为该配置对象启动 Consul 热更新，必须显式调用 `StartRetryConfigAutoReloadFromConsul(...)`。自动热更新任务是包级单例，不支持在同一进程中为多个独立配置对象分别监听不同配置。

## 9. 推荐实践

- 每个服务进程只初始化一次组件并复用默认客户端
- 写请求只有完成幂等保障后才通过精确规则开启重试
- 对外部依赖单独配置超时和重试策略
- 始终传递入站请求 Context
- 保持参与链路的服务节点时钟同步，确保绝对截止时间判断一致
- 同时监控重试率、重试成功率、最终失败率和请求耗时
- 发布配置前使用与组件相同的严格校验

常见故障见[故障排查](troubleshooting.md)。
