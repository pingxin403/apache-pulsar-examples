# Go Consumer ACK 机制示例

本示例演示 Apache Pulsar Consumer 的三种核心 ACK 机制：Individual ACK（单条确认）、Cumulative ACK（累积确认）和 Negative ACK（否定确认）。

## 📋 功能说明

Consumer ACK 机制是 Pulsar 消息消费的核心概念，决定了消息何时被标记为"已消费"。不同的 ACK 机制适用于不同的业务场景：

- **Individual ACK**: 单独确认每条消息，适合需要精确控制的场景
- **Cumulative ACK**: 批量确认消息，适合顺序处理场景
- **Negative ACK**: 明确告知消息处理失败，触发快速重试

## 🚀 快速开始

### 前置条件

- Go 1.19 or higher
- Pulsar running locally (see `../../docker-compose/`)

### 1. 启动 Pulsar

```bash
cd ../../docker-compose
docker-compose up -d
```

验证 Pulsar 是否启动成功：
```bash
docker exec pulsar bin/pulsar-admin brokers healthcheck
```

### 2. 安装依赖

```bash
go mod download
```

### 3. 运行示例

#### Individual ACK 示例
```bash
go run individual_ack.go
```

#### Cumulative ACK 示例
```bash
go run cumulative_ack.go
```

#### Negative ACK 示例
```bash
go run negative_ack.go
```

## 📚 示例详解

### 1. Individual ACK (`individual_ack.go`)

**核心代码**：
```go
consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    Topic:            "persistent://public/default/individual-ack-topic",
    SubscriptionName: "individual-ack-subscription",
    Type:             pulsar.Shared, // Shared 订阅
})

for messageCount < maxMessages {
    msg, err := consumer.Receive(ctx)
    
    content := string(msg.Payload())
    err = processMessage(content)
    
    if err != nil {
        // 处理失败，不确认消息
        fmt.Println("⚠️ 消息未确认，将被重新投递")
    } else {
        // 处理成功，单独确认这条消息
        consumer.Ack(msg)
        fmt.Println("✅ 消息已确认")
    }
}
```

**特点**：
- ✅ 精确控制：可以单独确认或拒绝每条消息
- ✅ 灵活性高：适合 Shared 订阅模式
- ✅ 容错性好：失败的消息不影响其他消息
- ❌ 网络开销：每条消息都需要一次 ACK 请求

**适用场景**：
- Shared 订阅模式（多个 Consumer 并行消费）
- 消息处理可能失败的场景
- 需要精确控制每条消息状态的场景
- 消息之间没有顺序依赖的场景

**预期输出**：
```
🚀 Consumer 已启动，使用 Individual ACK 模式
📌 订阅类型: Shared（支持多个 Consumer 并行消费）
📌 Individual ACK 会单独确认每条消息

📩 收到消息 #1: Hello Pulsar
✅ 消息处理成功: Hello Pulsar
✅ 消息已确认: {1 0 -1 0 [] <nil>}

📩 收到消息 #2: error
❌ 消息处理失败: 业务处理失败
⚠️ 消息未确认，将被重新投递

📊 Individual ACK 特点:
✅ 精确控制：可以单独确认或拒绝每条消息
✅ 灵活性高：适合 Shared 订阅模式
✅ 容错性好：失败的消息不影响其他消息
❌ 网络开销：每条消息都需要一次 ACK 请求
```

### 2. Cumulative ACK (`cumulative_ack.go`)

**核心代码**：
```go
consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    Topic:            "persistent://public/default/cumulative-ack-topic",
    SubscriptionName: "cumulative-ack-subscription",
    Type:             pulsar.Exclusive, // 使用 Exclusive 订阅
})

batchSize := 3 // 每处理 3 条消息，执行一次 Cumulative ACK

for messageCount < maxMessages {
    msg, err := consumer.Receive(ctx)
    messageCount++
    
    content := string(msg.Payload())
    processMessage(content)
    
    // 每处理 batchSize 条消息，执行一次 Cumulative ACK
    if messageCount%batchSize == 0 {
        consumer.AckCumulative(msg)
        fmt.Printf("✅ Cumulative ACK: 已确认消息 #1 到 #%d\n", messageCount)
    } else {
        fmt.Println("⏳ 暂不确认，等待批量确认")
    }
}
```

**特点**：
- ✅ 高效率：减少网络开销（批量确认）
- ✅ 简单易用：适合顺序处理场景
- ❌ 仅限 Exclusive/Failover：不支持 Shared 订阅
- ❌ 容错性差：某条消息失败会阻塞后续消息

**配置说明**：
- 必须使用 `pulsar.Exclusive` 或 `pulsar.Failover` 订阅模式
- `consumer.AckCumulative(msg)` 会确认当前消息及之前的所有消息
- 适合按顺序处理消息的场景

**适用场景**：
- Exclusive 或 Failover 订阅模式
- 消息必须按顺序处理的场景
- 消息处理成功率高的场景
- 需要降低网络开销的场景

**预期输出**：
```
🚀 Consumer 已启动，使用 Cumulative ACK 模式
📌 订阅类型: Exclusive（确保消息顺序处理）
📌 Cumulative ACK 会确认当前消息及之前的所有消息

📩 收到消息 #1: Message 1
✅ 消息处理成功: Message 1
⏳ 暂不确认，等待批量确认

📩 收到消息 #2: Message 2
✅ 消息处理成功: Message 2
⏳ 暂不确认，等待批量确认

📩 收到消息 #3: Message 3
✅ 消息处理成功: Message 3
✅ Cumulative ACK: 已确认消息 #1 到 #3
💡 网络开销: 1 次 ACK 请求确认了 3 条消息

📊 性能对比:
Individual ACK: 10 条消息 = 10 次网络请求
Cumulative ACK: 10 条消息 = 4 次网络请求（每 3 条确认一次）
网络开销降低: 60%
```

### 3. Negative ACK (`negative_ack.go`)

**核心代码**：
```go
consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    Topic:                "persistent://public/default/negative-ack-topic",
    SubscriptionName:     "negative-ack-subscription",
    Type:                 pulsar.Shared,
    NackRedeliveryDelay:  1 * time.Second, // 1 秒后重新投递
})

for messageCount < maxMessages {
    msg, err := consumer.Receive(ctx)
    
    content := string(msg.Payload())
    err = processMessage(content)
    
    if err != nil {
        // 判断错误类型
        if _, ok := err.(*RetryableError); ok {
            // 可重试的错误，发送 Nack
            consumer.Nack(msg)
            fmt.Println("🔄 已发送 Negative ACK，消息将在 1 秒后重新投递")
        } else {
            // 不可重试的错误，确认消息（避免无限重试）
            consumer.Ack(msg)
            fmt.Println("💡 不可重试的错误，确认消息以避免无限重试")
        }
    } else {
        consumer.Ack(msg)
    }
}
```

**特点**：
- ✅ 快速重试：立即触发消息重新投递
- ✅ 明确语义：区分"处理失败"和"不确认"
- ✅ 可配置延迟：控制重试间隔
- ❌ 需要判断：需要区分可重试和不可重试错误

**配置参数**：
- `NackRedeliveryDelay: 1 * time.Second`: 设置重新投递延迟

**适用场景**：
- 临时性错误（网络超时、服务暂时不可用）
- 需要快速重试的场景
- 需要区分可重试和不可重试错误的场景
- 避免消息长时间阻塞的场景

**预期输出**：
```
🚀 Consumer 已启动，使用 Negative ACK 模式
📌 订阅类型: Shared
📌 Negative ACK 会触发消息快速重新投递（1 秒延迟）

📩 收到消息 #1: timeout
⚠️ 消息处理失败，将重新投递: 网络超时
🔄 已发送 Negative ACK，消息将在 1 秒后重新投递

📩 收到消息 #2: timeout
⚠️ 消息处理失败，将重新投递: 网络超时
🔄 已发送 Negative ACK，消息将在 1 秒后重新投递

📩 收到消息 #3: invalid
❌ 消息无法处理，已确认: 数据格式错误
💡 不可重试的错误，确认消息以避免无限重试
```

## 🔍 ACK 机制对比

| 特性 | Individual ACK | Cumulative ACK | Negative ACK |
|------|---------------|----------------|--------------|
| 确认方式 | 单条确认 | 批量确认 | 否定确认 |
| 网络开销 | ❌ 高 | ✅ 低 | ✅ 低 |
| 订阅模式 | 所有模式 | Exclusive/Failover | 所有模式 |
| 容错性 | ✅ 高 | ❌ 低 | ✅ 高 |
| 顺序性 | ❌ 否 | ✅ 是 | ❌ 否 |
| 重试控制 | 被动 | 被动 | 主动 |
| 适用场景 | Shared 订阅 | 顺序处理 | 快速重试 |

## 💡 最佳实践

### 1. 选择合适的 ACK 机制

- **Shared 订阅 + 并行处理**：使用 Individual ACK
- **Exclusive 订阅 + 顺序处理**：使用 Cumulative ACK
- **临时性错误**：使用 Negative ACK

### 2. 异常处理策略

```go
err := processMessage(content)
if err != nil {
    // 判断错误类型
    if _, ok := err.(*RetryableError); ok {
        // 可重试错误：发送 Nack
        consumer.Nack(msg)
    } else {
        // 不可重试错误：确认消息，避免无限重试
        consumer.Ack(msg)
        log.Printf("Permanent error: %v", err)
    }
} else {
    consumer.Ack(msg)
}
```

### 3. 资源管理

使用 defer 确保资源释放：
```go
client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    Topic:            "my-topic",
    SubscriptionName: "my-subscription",
})
if err != nil {
    log.Fatal(err)
}
defer consumer.Close()
```

### 4. Context 管理

使用 context 控制超时和取消：
```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

msg, err := consumer.Receive(ctx)
if err != nil {
    if err == context.DeadlineExceeded {
        log.Println("接收消息超时")
    }
    return
}
```

### 5. Negative ACK 配置建议

```go
consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    Topic:                "my-topic",
    SubscriptionName:     "my-subscription",
    Type:                 pulsar.Shared,
    NackRedeliveryDelay:  1 * time.Second, // 根据业务调整重试延迟
})
```

### 6. Cumulative ACK 注意事项

- 只能用于 Exclusive 或 Failover 订阅模式
- 消息必须按顺序处理
- 某条消息失败会阻塞后续消息
- 适合消息处理成功率高的场景

## 🔧 故障排查

### 消息重复消费

**原因**：
- Consumer 崩溃前未确认消息
- ACK 超时
- 网络问题导致 ACK 丢失

**解决方案**：
```go
// 1. 设置合理的 ACK 超时时间
consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    Topic:            "my-topic",
    SubscriptionName: "my-subscription",
    AckTimeout:       30 * time.Second,
})

// 2. 实现幂等性处理
processedMessages := make(map[string]bool)

msg, err := consumer.Receive(ctx)
messageID := msg.ID().String()

if processedMessages[messageID] {
    consumer.Ack(msg)
    continue
}

processMessage(msg.Payload())
processedMessages[messageID] = true
consumer.Ack(msg)
```

### 消息丢失

**原因**：
- 消息被错误确认
- 没有配置重试机制

**解决方案**：
```go
// 谨慎使用 Ack，确保消息处理成功
err := processMessage(content)
if err != nil {
    // 不确认或发送 Nack
    consumer.Nack(msg)
} else {
    // 只在成功后确认
    consumer.Ack(msg)
}
```

### Cumulative ACK 失败

**错误信息**：
```
Cumulative ack cannot be used when the subscription type is Shared
```

**解决方案**：
```go
// 使用 Exclusive 或 Failover 订阅模式
consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    Topic:            "my-topic",
    SubscriptionName: "my-subscription",
    Type:             pulsar.Exclusive, // 或 pulsar.Failover
})
```

### 连接失败

```bash
# 检查 Pulsar 是否运行
docker ps | grep pulsar

# 检查 Pulsar 健康状态
docker exec pulsar bin/pulsar-admin brokers healthcheck

# 查看 Pulsar 日志
docker logs pulsar
```

## 📖 相关文档

- [Consumer ACK 机制详解](../../../Pulsar/01-入门篇/07-Consumer-ACK机制.md)
- [Pulsar Go Client 官方文档](https://pulsar.apache.org/docs/client-libraries-go/)
- [快速入门示例](../quickstart/)
- [Producer 发送模式](../producer-modes/)

## 🔗 完整代码

[查看完整代码示例](https://github.com/pingxin403/apache-pulsar-examples/tree/main/go-examples/consumer-ack)
