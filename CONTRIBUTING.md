# 贡献指南

感谢你为 HTTP Retry Governance Client 提交改进。

## 开发环境

- Go 版本以 [`go.mod`](go.mod) 中的 `go` 指令为准。
- 修改前请先阅读 [`README.md`](README.md) 和相关专题文档。
- 代码应保持简单，并遵循现有包结构和命名方式。

## 提交前检查

请在仓库根目录执行：

```bash
gofmt -w .
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...
```

涉及重试、超时、Consul 热更新或请求分类的变更，应包含能够复现旧行为并验证新行为的测试。修改配置模型时，还应同步更新：

- [`docs/config-reference.md`](docs/config-reference.md)
- [`examples/retry.minimal.yaml`](examples/retry.minimal.yaml)
- [`examples/consul/`](examples/consul/)

## 文档要求

- `README.md` 只保留安装、快速开始、关键约束和文档入口。
- 实际接入步骤放在 `docs/usage.md`。
- 配置字段和合并规则放在 `docs/config-reference.md`。
- 当前运行语义放在 `docs/retry-semantics.md`。
- 公开文档不得包含本机路径、内部域名、内部系统名称或未实现的设计。
- 内部方案和实施记录不应提交到公开仓库。
- Observer、Skipper、Resolver 或 Provider 的行为变更必须说明并发调用约束。

## 变更说明

对使用者可见的行为变更应记录在 [`CHANGELOG.md`](CHANGELOG.md) 的 `Unreleased` 章节。若修改模块路径，应同时更新 `go.mod`、文档和示例代码。
