# Go 订阅模式示例

本示例演示 Apache Pulsar 的 4 种订阅模式：Exclusive、Failover、Shared 和 Key_Shared。

## 📋 功能说明

| 订阅模式 | 文件 | 说明 |
|---------|------|------|
| **Exclusive** | `exclusive_subscription.go` | 独占模式，只有一个 Consumer 可以消费 |
| **Failover** | `failover_subscription.go` | 主备模式，Primary 故障时 Standby 自动接管 |
| **Shared** | `shared_subscription.go` | 共享模式，多个 Consumer 竞争消费 |
| **Key_Shared** | `key_shared_subscription.go` | 按 Key 分组，相同 Key 路由到同一 Consumer |

## 🚀 快速开始

### 前置条件

- Go 1.21+
- Pulsar running locally (see `../../docker-compose/`)

### 运行示例

```bash
# Exclusive 模式
go run exclusive_subscription.go

# Failover 模式
go run failover_subscription.go

# Shared 模式
go run shared_subscription.go

# Key_Shared 模式
go run key_shared_subscription.go
```

## 📖 相关文章

- [统一消息模型揭秘](../../../Pulsar/01-入门篇/04-统一消息模型.md)
- [Key_Shared 订阅实战](../../../Pulsar/03-核心功能深度篇/01-Key_Shared订阅实战.md)
