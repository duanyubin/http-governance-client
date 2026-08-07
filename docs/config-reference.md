# 配置参考

本文定义 YAML 配置结构、校验规则、分层合并和 Consul 热更新行为。接入步骤见[接入与使用](usage.md)，最终策略计算见[重试与链路语义](retry-semantics.md)。

## 1. 配置来源

组件支持三种配置来源：

- `SetRetryConfigProviderFromYAML(...)`：静态 YAML
- `SetupWithOptions(...)` / `SetRetryConfigProviderFromConsul(...)`：Consul 加载并热更新
- 自定义 `RetryConfigProvider`

Consul 按顺序读取：

```text
config/go/application/retry
config/go/{service}/retry
```

Key 不存在不视为初始化错误。读取失败、YAML 无效或配置校验失败会返回 error。

## 2. 完整结构

```yaml
version: v1

internal_hosts:
  - "*.svc.cluster.local"

downstreams:
  - name: "payments"
    hosts: ["payments.svc.cluster.local"]

operations:
  - name: "queryPayment"
    priority: 100
    methods: ["GET"]
    downstreams: ["payments"]
    paths: ["/v1/payments/*"]

policies:
  internal_read:
    max_retries: 1
    per_attempt_timeout: 2s
    max_elapsed_time: 4s
    initial_backoff: 100ms
    max_backoff: 400ms
    retry_on_statuses: [408, 429, 500, 502, 503, 504]
    retry_on_429: true

rules:
  - name: "disable-payment-query-retry"
    priority: 100
    match:
      callers: ["orders-api"]
      downstreams: ["payments"]
      operations: ["queryPayment"]
      classes: ["internal_read"]
    policy:
      disable_retry: true
```

YAML 使用严格字段校验，并且一份配置只能包含一个 YAML 文档。字段拼写错误、`hosts`/`paths` 中的无效 glob 或追加第二个 `---` 文档都会导致整份配置加载失败。

## 3. 顶层字段

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `version` | `string` | 建议 | 当前支持空值或 `v1`；建议固定为 `v1` |
| `internal_hosts` | `[]string` | 否 | 内部主机匹配规则 |
| `downstreams` | `[]object` | 否 | 下游名称识别规则 |
| `operations` | `[]object` | 否 | 业务操作识别规则 |
| `policies` | `object` | 否 | 四类请求的基线覆盖 |
| `rules` | `[]object` | 否 | 特定请求的策略覆盖 |

## 4. `internal_hosts`

支持精确 hostname、`host:port` 和 Go `path.Match` 风格的 glob：

```yaml
internal_hosts:
  - "*.svc.cluster.local"
  - "payments"
  - "payments:8080"
  - "localhost"
```

使用内置 YAML 或 Consul 配置时：

- 命中 → `internal_read` 或 `internal_write`
- 未命中 → `external_read` 或 `external_write`
- 列表为空 → 所有有效目标主机均按 external 分类

读方法为 `GET`、`HEAD`、`OPTIONS`、`TRACE`，其他方法按写请求处理。显式 Context 分类的优先级高于自动分类。

## 5. `downstreams`

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `name` | `string` | 是 | 稳定的下游名称 |
| `hosts` | `[]string` | 是 | 至少包含一个 host 匹配规则 |

```yaml
downstreams:
  - name: "payments"
    hosts:
      - "payments.svc.cluster.local"
      - "api.example.com/payments"
  - name: "fraud-api"
    hosts:
      - "fraud.example.net"
```

`hosts` 支持：

- hostname
- `host:port`
- glob
- `host + 一级 path 段`，例如 `api.example.com/payments`

按本次请求实际命中的模式计算具体度：精确匹配优先于 glob，`host/一级路径` 优先于仅 host；具体度相同时按配置顺序命中。一个下游配置包含多个宽泛模式不会因此获得更高优先级。

Host 和一级 URL Path 都不区分大小写。

## 6. `operations`

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `name` | `string` | 是 | 操作名称 |
| `priority` | `int` | 否 | 默认 `0`，数值越大越优先 |
| `methods` | `[]string` | 否 | HTTP 方法 |
| `hosts` | `[]string` | 否 | host 匹配 |
| `paths` | `[]string` | 否 | URL path glob |
| `downstreams` | `[]string` | 否 | 下游名称 |

同一规则中已填写的条件必须全部命中；单个字段中的多个值是“任一命中”。`hosts` 和 `paths` 支持 glob，`methods` 和 `downstreams` 只做不区分大小写的精确匹配。同优先级时，填写匹配维度更多的规则优先。

```yaml
operations:
  - name: "queryPayment"
    priority: 100
    methods: ["GET"]
    downstreams: ["payments"]
    paths: ["/v1/payments/*"]
```

`paths` 使用 Go `path.Match` 风格的 glob，匹配时不区分大小写，也不支持 Gin 风格参数：

```text
支持：/v1/payments/*
不支持：/v1/payments/:id
```

没有命中 operation 时，组件使用 `METHOD + URL.Path` 作为回退名称，例如 `GET /v1/orders/123`。回退值不包含 scheme、host、query 或 fragment。

## 7. `policies`

支持以下分类：

- `internal_read`
- `internal_write`
- `external_read`
- `external_write`

内置默认值：

| 字段 | 读请求 | 写请求 |
| --- | --- | --- |
| `max_retries` | `1` | `0` |
| `per_attempt_timeout` | `2s` | `2s` |
| `max_elapsed_time` | 未设置 | 未设置 |
| `initial_backoff` | `100ms` | `100ms` |
| `max_backoff` | `2s` | `2s` |
| `retry_on_statuses` | `[408,429,500,502,503,504]` | 同左 |
| `retry_on_429` | `true` | `true` |

动态配置只覆盖已填写字段。

### 7.1 策略字段

| 字段 | 类型 | 校验 | 说明 |
| --- | --- | --- | --- |
| `disable_retry` | `bool` | `true` 不能与 `max_retries` 同时配置 | 强制关闭自动重试 |
| `max_retries` | `int` | 必须 `>= 0` | 额外重试次数，不含首次请求 |
| `per_attempt_timeout` | Go duration | 必须 `> 0` | 单次尝试超时 |
| `max_elapsed_time` | Go duration | 必须 `> 0` | HTTP 首次尝试、退避和重试的整体预算；同步扩展回调不受该预算强制中断 |
| `initial_backoff` | Go duration | 必须 `> 0` | 第一次重试前的基础退避 |
| `max_backoff` | Go duration | 必须 `> 0` | 随机抖动后的退避上限 |
| `retry_on_statuses` | `[]int` | 可为空数组；每项必须为 `100..599` | 可重试 HTTP 状态码集合 |
| `retry_on_429` | `bool` | 无 | 是否最终允许重试 `429` |

`disable_retry: false` 可以与 `max_retries` 同时出现，但通常无需显式填写 `false`。`disable_retry: true` 与 `max_retries` 同时出现会导致加载失败。

`retry_on_statuses: []` 表示不根据任何 HTTP 状态码重试，不会回退到内置集合。`429` 只有同时位于候选集合且 `retry_on_429: true` 时才会重试。

`downstreams`、`operations` 以及非空的 `rules.name` 在同一份 YAML 中必须唯一（名称比较不区分大小写）。重复状态码会在加载时去重。

## 8. `rules`

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `name` | `string` | 建议 | 用于合并、审计和排障 |
| `priority` | `int` | 否 | 默认 `0`，数值越大越优先 |
| `disabled` | `bool` | 否 | 禁用该规则 |
| `match` | `object` | 启用时是 | 至少包含一个有效匹配值 |
| `policy` | `object` | 启用时是 | 至少覆盖一个策略字段 |

`match` 支持以下字段。各字段均可省略，但启用的规则必须至少包含一个有效匹配值；空数组和仅包含空白字符串的数组不计为有效值。

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `callers` | `[]string` | 否 | 调用方服务名称，不区分大小写的精确匹配 |
| `downstreams` | `[]string` | 否 | 下游名称，不区分大小写的精确匹配 |
| `operations` | `[]string` | 否 | 操作名称，不区分大小写的精确匹配 |
| `methods` | `[]string` | 否 | HTTP 方法，不区分大小写的精确匹配 |
| `hosts` | `[]string` | 否 | 请求 URL Host，可包含端口；支持 glob 且不区分大小写 |
| `paths` | `[]string` | 否 | 请求 URL Path，不包含 query；支持 glob 且不区分大小写 |
| `classes` | `[]string` | 否 | 请求分类：`internal_read`、`internal_write`、`external_read`、`external_write` |

同一字段中的多个值是“任一命中”，不同字段之间必须全部命中。只有 `hosts` 和 `paths` 支持 glob。旧的单数字段名不再支持，严格解析会将其视为未知字段。

规则先按 `priority` 降序排列，再按匹配维度数量降序排列。只应用第一条命中的规则。

```yaml
rules:
  - name: "retry-idempotent-payment"
    priority: 120
    match:
      callers: ["orders-api"]
      downstreams: ["payments"]
      operations: ["createPayment"]
      methods: ["POST"]
      classes: ["internal_write"]
    policy:
      max_retries: 1
      max_elapsed_time: 3s
```

只有具备幂等键、唯一请求号或等效去重能力的写操作才应启用自动重试。

## 9. 应用级与服务级合并

| 配置部分 | 合并方式 |
| --- | --- |
| `version` | 服务级非空值优先 |
| `internal_hosts` | 两层列表合并并去重；服务级不能通过省略删除应用级值 |
| `downstreams` | 按 `name` 匹配；服务级同名项整体替换应用级项 |
| `operations` | 按 `name` 匹配；服务级同名项整体替换应用级项 |
| `policies` | 分类内按字段合并，服务级已填写字段优先 |
| `rules` | 按 `name` 匹配；服务级同名项整体替换应用级项 |

需要在服务级删除应用级规则时，可以放置同名的禁用规则：

```yaml
rules:
  - name: "application-rule-name"
    disabled: true
```

`internal_hosts`、`downstreams` 和 `operations` 没有通用删除标记。若需要删除应用级条目，应修改应用级 Key。

## 10. 最终优先级

策略按以下顺序计算：

1. 内置默认策略
2. Context 中的基础策略
3. 应用级分类策略
4. 服务级分类策略
5. 第一条命中的 rule
6. 上游禁止重试约束
7. Context deadline、`X-Request-Deadline`、`max_elapsed_time` 中最早的截止时间
8. Body 可重放性和剩余预算

前五层可以放宽或收紧当前调用策略；后三层只能收紧。

## 11. 热更新行为

- Consul Key 创建、更新和删除都会触发重新加载
- 每次重新加载都会读取应用级和服务级两个 Key
- 新配置会先完成严格解析和编译，再原子替换当前生效配置
- 加载失败时继续使用上一份有效配置
- 服务级 Key 删除后回退到应用级配置
- 两个 Key 都不存在时动态配置清空，继续使用内置默认策略

两个 Key 不构成 Consul 事务快照；同时修改时可能短暂读取到不同版本，后续 watch 会再次加载并收敛。

内置 Consul 客户端接受 HTTP/HTTPS endpoint，但当前没有单独的 ACL Token、客户端证书或自定义 CA 配置项。需要这些能力时，应由运行环境提供可直接访问的 endpoint，或使用自定义 `RetryConfigProvider`。
