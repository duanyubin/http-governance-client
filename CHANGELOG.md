# 变更日志

本项目的重要变更将记录在此文件中。

## [Unreleased]

### 新增

- 增加可选的 Prometheus 指标适配器，提供请求量、重试、失败、超时和耗时指标。
- 增加可选的按 downstream 隔离的本地重试预算，并提供预算抑制日志、结果事件和 Prometheus 指标。

## [0.6.0] - 2026-08-07

### 变更

- `rules[].match` 的集合字段统一为复数：`callers`、`downstreams`、`operations`、`methods`、`hosts`、`paths`、`classes`；旧的单数字段不再支持。

## [0.5.1] - 2026-08-06

### 修复

- 修复请求未设置 `Content-Type` 时，小请求体无法打印日志的问题。

## [0.5.0] - 2026-08-06

### 新增

- 新增 `GetWithHeaders`、`PostWithHeaders` 等统一的函数式请求 Header API。

### 兼容性

- 保留现有 `Internal*` 方法和 `AuthorizationInHeaderSetter`，并复用统一请求实现。

## [0.4.0] - 2026-08-04

### 新增

- Body 日志识别 `application/x-protobuf` 内容类型。

## [0.3.0] - 2026-08-04

### 新增

- Body 日志读取上限支持通过 `SetBodyLogLimit(...)` 调整，默认值保持 64 KiB。

### 修复

- 修复未知 `Content-Length` 的可读响应 Body 被跳过，以及 JSON 数组、标量或无效 JSON 不产生 Body 日志的问题。

## [0.2.1] - 2026-08-04

### 修复

- 修复 Windows 平台连接被拒绝错误未被识别为可重试传输错误的问题。

## [0.2.0] - 2026-08-04

### 变更

- 仓库和 Go module 地址更新为 `github.com/duanyubin/http-governance-client`。
- 重新整理日志、观测和请求体重放的接入说明。

## [0.1.0] - 2026-07-30

### 新增

- 支持通过 YAML 或 Consul 配置请求分类、下游、操作和重试策略。
- 支持 `application -> service` 两级 Consul 配置合并及自动热更新。
- 支持 `X-No-More-Retry` 和 `X-Request-Deadline` 跨服务传递链路约束。
- 提供 Gin 中间件，用于接收链路约束并写入请求上下文。
- 提供重试事件和最终请求结果观察接口。
- 增加配置参考、运行语义、故障排查和通用配置示例。
- 支持独立注册 `RetryResultObserver`，并保留组合 Observer 的兼容行为。

### 变更

- 配置更新改为校验成功后原子替换；无效更新保留上一份有效配置。
- Consul 监听支持配置键新增、修改和删除；服务配置删除后回退到应用配置。
- YAML 解析启用未知字段校验，并补充下游、操作和退避参数校验。
- YAML 只接受单文档，拒绝重复逻辑名称和 `100..599` 之外的 HTTP 状态码。
- 下游识别按本次实际命中的模式具体度选择，不再受同一条目模式数量影响。
- `max_backoff` 现在约束加入随机抖动后的最终等待时间。
- `ResponsePtr` 增加拼写正确的 `ExpectedPtr`；原 `ExceptPtr` 保留用于兼容。
- 公开文档和示例统一为面向外部开发者的内容，内部设计记录不再随仓库发布。

### 修复

- 修复并发更新共享 Transport 时可能产生的数据竞争。
- 修复重试请求体关闭和重放生命周期问题。
- 修复 Consul 配置删除、空配置及监听索引变化时的热更新行为。
- 修复请求总预算、单次尝试超时和退避等待可能突破链路截止时间的问题。
- 修复健康检查跳过规则、multipart boundary 解析和空响应体处理。
- 修复 `Retry-After`、指数退避溢出以及无目标响应体未排空的问题。
- 修复替换动态配置 Provider 后旧请求分类器仍然生效的问题。
- 升级存在可达安全漏洞的 `quic-go` 间接依赖。
- 修复网络超时未计入 `timedOut`，以及未使用 Managed Provider 时结果日志统计维度为空的问题。
- 未配置 operation 时改为回退到 `METHOD + URL.Path`，避免仅使用 path 最后片段导致接口含义丢失。
- 修复 Body 日志预读无硬上限且吞掉读取错误的问题，确保日志功能不改变请求或响应语义。
- 修复独立 Client 的自定义请求分类晚于基础策略解析，以及自定义 Provider 收到未补齐 Scope 的问题。
- 统一 Host 与 URL Path 的精确匹配和 glob 匹配均不区分大小写，并仅对支持 glob 的 host/path 在配置加载阶段校验语法。

[Unreleased]: https://github.com/duanyubin/http-governance-client/compare/v0.5.1...HEAD
[0.5.1]: https://github.com/duanyubin/http-governance-client/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/duanyubin/http-governance-client/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/duanyubin/http-governance-client/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/duanyubin/http-governance-client/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/duanyubin/http-governance-client/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/duanyubin/http-governance-client/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/duanyubin/http-governance-client/releases/tag/v0.1.0
