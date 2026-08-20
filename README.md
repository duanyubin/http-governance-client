# HTTP Retry Governance Client

基于 Go `net/http` 的客户端治理组件，提供有限重试、超时预算、请求分类、Consul 动态配置、链路约束传播和请求结果观测能力。

## 环境要求

- Go 版本以 [`go.mod`](go.mod) 为准
- Gin 中间件是可选能力；仅使用标准 `net/http` 时无需注册
- Consul 是可选配置源；也可以直接加载 YAML 或自定义 `RetryConfigProvider`

## 核心能力

- 内部读、内部写、外部读、外部写四类请求策略
- 单次超时、整体预算、指数退避和随机抖动
- 可选的按 downstream 隔离的本地重试预算，用于限制持续故障时的额外重试
- 基于 `caller / downstream / operation / class` 的动态规则
- Consul blocking query 热更新
- 跨服务传播 `X-No-More-Retry` 和 `X-Request-Deadline`
- 单跳维护 `X-Retry-Attempt` 和 `X-Retry-Reason`
- `slog` 日志及 `RetryObserver` / `RetryResultObserver`
- 可选的 Prometheus 请求量、重试、预算抑制、失败、超时和耗时指标

## 安装

```bash
go get github.com/duanyubin/http-governance-client
```

包名为 `http`，建议使用别名导入，避免与标准库冲突：

```go
import httpclient "github.com/duanyubin/http-governance-client"
```

## 快速开始

### 1. 初始化

服务启动时显式传入服务名和 Consul 地址：

```go
func initHTTP(consulAddress string) error {
	return httpclient.SetupWithOptions(httpclient.SetupOptions{
		Name:   "orders-api",
		Consul: consulAddress,
	})
}
```

初始化会读取以下 Consul KV，并启动热更新：

1. `config/go/application/retry`
2. `config/go/orders-api/retry`

服务级配置在应用级配置之后合并。没有 Consul 时，可通过
`SetRetryConfigProviderFromYAML(...)` 加载静态配置。

`Setup()` 是读取 `--env`、`--name`、`--consul` 进程参数的便捷入口。库代码和测试通常优先使用显式的 `SetupWithOptions(...)`。

### 2. Gin 链路约束

```go
import ginmiddleware "github.com/duanyubin/http-governance-client/ginmiddleware"

router.Use(ginmiddleware.GovernanceMiddleware())
```

Handler 调用下游时传递标准请求 Context：

```go
ctx := c.Request.Context()
err := httpclient.Post(
	ctx,
	"http://payments.svc.cluster.local/v1/payments",
	"application/json",
	body,
	&response,
)
```

需要从入站请求读取 Token 或设置其他业务 Header 时，使用对应的 `*WithHeaders` 方法：

```go
err := httpclient.GetWithHeaders(
	c.Request.Context(),
	targetURL,
	&response,
	func(req *http.Request) error {
		req.Header.Set("Authorization", c.GetHeader("Authorization"))
		return nil
	},
)
```

辅助函数不是必选项。也可以用 `http.NewRequestWithContext(...)` 创建请求，再通过 `httpclient.Do(...)` 或 `httpclient.Instance().Do(...)` 发送；不要改用 `http.DefaultClient`，具体见[接入与使用](docs/usage.md#35-使用标准-httprequest)。

不要传递 `*gin.Context`，也不要用 `context.Background()` 替换入站请求 Context。

### 3. 处理 HTTP 状态码

辅助函数不会把 HTTP `4xx/5xx` 自动转换为 Go error。需要同时检查返回的 error 和状态码：

```go
var body PaymentResponse
result := httpclient.ResponsePtr{ExpectedPtr: &body}

if err := httpclient.Get(ctx, targetURL, &result); err != nil {
	return err // 传输、超时或解码错误
}
if result.StatusCode < 200 || result.StatusCode >= 300 {
	return fmt.Errorf("payments returned HTTP %d", result.StatusCode)
}
```

### 4. Prometheus 指标（可选）

需要直接采集指标时，在服务启动时注册内置适配器：

```go
import (
	httpclient "github.com/duanyubin/http-governance-client"
	prometheusmetrics "github.com/duanyubin/http-governance-client/metrics/prometheus"
	"github.com/gin-gonic/gin"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

observer, err := prometheusmetrics.New(stdprometheus.DefaultRegisterer)
if err != nil {
	return err
}
httpclient.SetRetryResultObserver(observer)
router.GET("/metrics", gin.WrapH(promhttp.Handler()))
```

`/metrics` 端点由应用暴露。指标明细、独立 Registry 和 PromQL 示例见[接入与使用](docs/usage.md#74-prometheus-指标)。

## 重试安全约束

- 读请求默认最多额外重试一次
- 写请求默认不自动重试
- 写请求只有在具备幂等保障后，才应通过精确规则开启重试
- 带 Body 的请求只有在请求体可重放时才会自动重试
  - 使用 `Post`、`PostWithHeaders` 等辅助函数时，组件已生成可重放的请求体，无需额外处理
  - 直接创建 `http.Request` 时，`bytes.Buffer`、`bytes.Reader` 和 `strings.Reader` 由标准库自动设置 `GetBody`
  - 使用其他自定义或流式 Body 时，需要自行设置 `GetBody`；未设置时组件会关闭该请求的自动重试，只发送一次
- HTTP 尝试与退避受 `max_elapsed_time`、请求 Context deadline 和 `X-Request-Deadline` 中最早的截止时间约束
- 上游 `X-No-More-Retry=true` 不能被下游配置重新放宽
- 可通过 `retry_budget` 限制每个 downstream 的额外重试；该预算不限制首次请求，也不替代幂等、限流或服务端过载保护

## 文档

- [接入与使用](docs/usage.md)
- [配置参考](docs/config-reference.md)
- [重试与链路语义](docs/retry-semantics.md)
- [故障排查](docs/troubleshooting.md)
- [应用级配置示例](examples/consul/application.retry.yaml)
- [服务级配置示例](examples/consul/service.retry.yaml)
- [最小配置示例](examples/retry.minimal.yaml)

## 开发

```bash
go test ./...
go test -race ./...
go vet ./...
```

协作约定见 [CONTRIBUTING.md](CONTRIBUTING.md)，安全问题报告方式见 [SECURITY.md](SECURITY.md)，版本记录见 [CHANGELOG.md](CHANGELOG.md)，开源协议见 [LICENSE](LICENSE)。
