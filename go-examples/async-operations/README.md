# Go 异步操作示例

本示例演示 Apache Pulsar Go 客户端的异步操作，包括异步发送和基于 Channel 的异步消费。

## 📋 功能说明

| 示例 | 文件 | 说明 |
|------|------|------|
| **异步 Producer** | `async_producer.go` | 使用 SendAsync 实现非阻塞高吞吐发送 |
| **异步 Consumer** | `async_consumer.go` | 使用 MessageChannel + goroutine 并发消费 |

## 🚀 快速开始

### 前置条件

- Go 1.21+
- Pulsar running locally (see `../../docker-compose/`)

### 运行示例

```bash
# 异步 Producer（先运行）
go run async_producer.go

# 异步 Consumer
go run async_consumer.go
```

## 📖 相关文章

- [Producer 发送模式全解析](../../../Pulsar/01-入门篇/06-Producer发送模式.md)
- [客户端最佳实践](../../../Pulsar/01-入门篇/10-客户端最佳实践.md)
