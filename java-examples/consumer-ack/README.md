# Consumer ACK 机制示例

本示例演示 Apache Pulsar Consumer 的三种核心 ACK 机制：Individual ACK（单条确认）、Cumulative ACK（累积确认）和 Negative ACK（否定确认），以及 Dead Letter Queue（死信队列）的使用。

## 📋 功能说明

Consumer ACK 机制是 Pulsar 消息消费的核心概念，决定了消息何时被标记为"已消费"。不同的 ACK 机制适用于不同的业务场景：

- **Individual ACK**: 单独确认每条消息，适合需要精确控制的场景
- **Cumulative ACK**: 批量确认消息，适合顺序处理场景
- **Negative ACK**: 明确告知消息处理失败，触发快速重试
- **Dead Letter Queue**: 处理无法消费的"毒消息"，避免阻塞

## 🚀 快速开始

### 前置条件

- JDK 11 or higher
- Maven 3.6+
- Pulsar running locally (see `../../docker-compose/`)

### 1. 启动 Pulsar

```bash
cd ../../docker-compose
docker-compose up -d
```

验证 Pulsar 是否启动成功：
```bash
docker exec pulsar-standalone bin/pulsar-admin brokers healthcheck
```

### 2. 编译项目

```bash
mvn clean package
```

### 3. 运行示例

#### Individual ACK 示例
```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.IndividualAckExample"
```

#### Cumulative ACK 示例
```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.CumulativeAckExample"
```

#### Negative ACK 示例
```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.NegativeAckExample"
```

#### Dead Letter Queue 示例
```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.DeadLetterQueueExample"
```

## 📚 示例详解

### 1. Individual ACK (`IndividualAckExample.java`)

**核心代码**：
```java
Consumer<String> consumer = client.newConsumer(Schema.STRING)
    .topic("persistent://public/default/my-topic")
    .subscriptionName("my-subscription")
    .subscriptionType(SubscriptionType.Shared)  // Shared 订阅
    .subscribe();

while (true) {
    Message<String> msg = consumer.receive();
    
    try {
        processMessage(msg.getValue());
        
        // 单独确认这条消息
        consumer.acknowledge(msg);
        System.out.println("✅ 消息已确认: " + msg.getMessageId());
        
    } catch (Exception e) {
        System.err.println("❌ 消息处理失败: " + e.getMessage());
        // 不确认，消息会被重新投递
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
📩 收到消息: Hello Pulsar
✅ 消息已确认: 1:0:-1:0
📩 收到消息: error message
❌ 消息处理失败: 业务处理失败
```

### 2. Cumulative ACK (`CumulativeAckExample.java`)

**核心代码**：
```java
Consumer<String> consumer = client.newConsumer(Schema.STRING)
    .topic("persistent://public/default/cumulative-ack-topic")
    .subscriptionName("cumulative-subscription")
    .subscriptionType(SubscriptionType.Exclusive)  // 使用 Exclusive 订阅
    .subscribe();

int messageCount = 0;

while (messageCount < 10) {
    Message<String> msg = consumer.receive();
    messageCount++;
    
    processMessage(msg.getValue());
    
    // 每处理 3 条消息，执行一次 Cumulative ACK
    if (messageCount % 3 == 0) {
        consumer.acknowledgeCumulative(msg);
        System.out.println("✅ Cumulative ACK: 已确认消息 #1 到 #" + messageCount);
    }
}
```

**特点**：
- ✅ 高效率：减少网络开销（批量确认）
- ✅ 简单易用：适合顺序处理场景
- ❌ 仅限 Exclusive/Failover：不支持 Shared 订阅
- ❌ 容错性差：某条消息失败会阻塞后续消息

**配置说明**：
- 必须使用 `Exclusive` 或 `Failover` 订阅模式
- `acknowledgeCumulative(msg)` 会确认当前消息及之前的所有消息
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
⏳ 暂不确认，等待批量确认

📩 收到消息 #2: Message 2
⏳ 暂不确认，等待批量确认

📩 收到消息 #3: Message 3
✅ Cumulative ACK: 已确认消息 #1 到 #3
💡 网络开销: 1 次 ACK 请求确认了 3 条消息

📊 性能对比:
Individual ACK: 10 条消息 = 10 次网络请求
Cumulative ACK: 10 条消息 = 4 次网络请求（每 3 条确认一次）
网络开销降低: 60%
```

### 3. Negative ACK (`NegativeAckExample.java`)

**核心代码**：
```java
Consumer<String> consumer = client.newConsumer(Schema.STRING)
    .topic("persistent://public/default/my-topic")
    .subscriptionName("my-subscription")
    .subscriptionType(SubscriptionType.Shared)
    .negativeAckRedeliveryDelay(1, TimeUnit.SECONDS)  // 1 秒后重新投递
    .subscribe();

while (true) {
    Message<String> msg = consumer.receive();
    
    try {
        processMessage(msg.getValue());
        consumer.acknowledge(msg);
        
    } catch (RetryableException e) {
        // 可重试的错误，发送 Nack
        System.err.println("⚠️ 消息处理失败，将重新投递: " + e.getMessage());
        consumer.negativeAcknowledge(msg);
        
    } catch (Exception e) {
        // 不可重试的错误，确认消息（避免无限重试）
        System.err.println("❌ 消息无法处理，已确认: " + e.getMessage());
        consumer.acknowledge(msg);
    }
}
```

**特点**：
- ✅ 快速重试：立即触发消息重新投递
- ✅ 明确语义：区分"处理失败"和"不确认"
- ✅ 可配置延迟：控制重试间隔
- ❌ 需要判断：需要区分可重试和不可重试错误

**配置参数**：
- `negativeAckRedeliveryDelay(1, TimeUnit.SECONDS)`: 设置重新投递延迟

**适用场景**：
- 临时性错误（网络超时、服务暂时不可用）
- 需要快速重试的场景
- 需要区分可重试和不可重试错误的场景
- 避免消息长时间阻塞的场景

**预期输出**：
```
📩 收到消息: timeout message
⚠️ 消息处理失败，将重新投递: 网络超时
📩 收到消息: timeout message
⚠️ 消息处理失败，将重新投递: 网络超时
📩 收到消息: invalid message
❌ 消息无法处理，已确认: 数据格式错误
```

### 4. Dead Letter Queue (`DeadLetterQueueExample.java`)

**核心代码**：
```java
Consumer<String> consumer = client.newConsumer(Schema.STRING)
    .topic("persistent://public/default/my-topic")
    .subscriptionName("my-subscription")
    .subscriptionType(SubscriptionType.Shared)
    .ackTimeout(10, TimeUnit.SECONDS)
    .deadLetterPolicy(DeadLetterPolicy.builder()
        .maxRedeliverCount(3)  // 最多重试 3 次
        .deadLetterTopic("persistent://public/default/my-topic-DLQ")  // 死信队列
        .build())
    .subscribe();

while (true) {
    Message<String> msg = consumer.receive();
    
    try {
        processMessage(msg.getValue());
        consumer.acknowledge(msg);
        
    } catch (Exception e) {
        System.err.println("❌ 消息处理失败: " + e.getMessage());
        // 发送 Nack，触发重试
        consumer.negativeAcknowledge(msg);
        // 如果重试次数超过 3 次，消息会自动发送到 DLQ
    }
}
```

**特点**：
- ✅ 避免阻塞：无法处理的消息不会阻塞队列
- ✅ 可追溯：死信消息可以单独分析和处理
- ✅ 自动化：超过重试次数自动转移到 DLQ
- ❌ 需要监控：需要监控 DLQ 中的消息

**配置参数**：
- `maxRedeliverCount(3)`: 最大重试次数
- `deadLetterTopic("...")`: 死信队列 Topic 名称
- `ackTimeout(10, TimeUnit.SECONDS)`: ACK 超时时间

**适用场景**：
- 处理"毒消息"（无法处理的消息）
- 需要保证消息队列不被阻塞的场景
- 需要单独分析失败消息的场景
- 生产环境的容错处理

**预期输出**：
```
📩 收到消息: poison message
❌ 消息处理失败: 毒消息，无法处理
📩 收到消息: poison message
❌ 消息处理失败: 毒消息，无法处理
📩 收到消息: poison message
❌ 消息处理失败: 毒消息，无法处理
📩 收到消息: poison message
❌ 消息处理失败: 毒消息，无法处理
💀 消息已发送到死信队列: persistent://public/default/my-topic-DLQ
```

## 🔍 ACK 机制对比

| 特性 | Individual ACK | Cumulative ACK | Negative ACK | Dead Letter Queue |
|------|---------------|----------------|--------------|-------------------|
| 确认方式 | 单条确认 | 批量确认 | 否定确认 | 自动转移 |
| 网络开销 | ❌ 高 | ✅ 低 | ✅ 低 | ✅ 低 |
| 订阅模式 | 所有模式 | Exclusive/Failover | 所有模式 | 所有模式 |
| 容错性 | ✅ 高 | ❌ 低 | ✅ 高 | ✅ 高 |
| 顺序性 | ❌ 否 | ✅ 是 | ❌ 否 | ❌ 否 |
| 重试控制 | 被动 | 被动 | 主动 | 自动 |
| 适用场景 | Shared 订阅 | 顺序处理 | 快速重试 | 毒消息处理 |

## 💡 最佳实践

### 1. 选择合适的 ACK 机制

- **Shared 订阅 + 并行处理**：使用 Individual ACK
- **Exclusive 订阅 + 顺序处理**：使用 Cumulative ACK
- **临时性错误**：使用 Negative ACK
- **无法处理的消息**：配置 Dead Letter Queue

### 2. 异常处理策略

```java
try {
    processMessage(msg.getValue());
    consumer.acknowledge(msg);
    
} catch (RetryableException e) {
    // 可重试错误：发送 Nack
    consumer.negativeAcknowledge(msg);
    
} catch (PermanentException e) {
    // 不可重试错误：确认消息，避免无限重试
    consumer.acknowledge(msg);
    // 记录错误日志，人工介入
    logger.error("Permanent error: ", e);
}
```

### 3. 资源管理

使用 try-with-resources 确保资源自动释放：
```java
try (PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();
     Consumer<String> consumer = client.newConsumer(Schema.STRING)
        .topic("my-topic")
        .subscriptionName("my-subscription")
        .subscribe()) {
    
    // 消费消息
    
} catch (PulsarClientException e) {
    logger.error("Pulsar client error: ", e);
}
// 自动关闭资源
```

### 4. Dead Letter Queue 配置建议

```java
.deadLetterPolicy(DeadLetterPolicy.builder()
    .maxRedeliverCount(3)           // 根据业务容忍度调整
    .deadLetterTopic("my-topic-DLQ") // 使用统一的命名规范
    .initialSubscriptionName("dlq-subscription") // 指定 DLQ 订阅名称
    .build())
.ackTimeout(10, TimeUnit.SECONDS)   // 设置合理的 ACK 超时时间
```

### 5. 监控 Dead Letter Queue

```bash
# 查看 DLQ 中的消息数量
pulsar-admin topics stats persistent://public/default/my-topic-DLQ

# 消费 DLQ 中的消息进行分析
pulsar-client consume persistent://public/default/my-topic-DLQ \
  -s "dlq-analysis" \
  -n 0
```

### 6. Cumulative ACK 注意事项

- 只能用于 Exclusive 或 Failover 订阅模式
- 消息必须按顺序处理
- 某条消息失败会阻塞后续消息
- 适合消息处理成功率高的场景

### 7. Negative ACK 配置建议

```java
.negativeAckRedeliveryDelay(1, TimeUnit.SECONDS)  // 根据业务调整重试延迟
.negativeAckRedeliveryBackoff(MultiplierRedeliveryBackoff.builder()
    .minDelayMs(1000)    // 最小延迟 1 秒
    .maxDelayMs(60000)   // 最大延迟 60 秒
    .multiplier(2.0)     // 指数退避倍数
    .build())
```

## 🔧 故障排查

### 消息重复消费

**原因**：
- Consumer 崩溃前未确认消息
- ACK 超时
- 网络问题导致 ACK 丢失

**解决方案**：
```java
// 1. 设置合理的 ACK 超时时间
.ackTimeout(30, TimeUnit.SECONDS)

// 2. 实现幂等性处理
String messageId = msg.getMessageId().toString();
if (processedMessages.contains(messageId)) {
    consumer.acknowledge(msg);
    continue;
}
processMessage(msg.getValue());
processedMessages.add(messageId);
consumer.acknowledge(msg);
```

### 消息丢失

**原因**：
- 消息被错误确认
- 没有配置 DLQ，消息被丢弃

**解决方案**：
```java
// 1. 谨慎使用 acknowledge，确保消息处理成功
try {
    processMessage(msg.getValue());
    consumer.acknowledge(msg);  // 只在成功后确认
} catch (Exception e) {
    // 不确认或发送 Nack
}

// 2. 配置 DLQ 避免消息丢失
.deadLetterPolicy(DeadLetterPolicy.builder()
    .maxRedeliverCount(3)
    .deadLetterTopic("my-topic-DLQ")
    .build())
```

### Cumulative ACK 失败

**错误信息**：
```
Cumulative ack cannot be used when the subscription type is Shared
```

**解决方案**：
```java
// 使用 Exclusive 或 Failover 订阅模式
.subscriptionType(SubscriptionType.Exclusive)
// 或
.subscriptionType(SubscriptionType.Failover)
```

### 连接失败

```bash
# 检查 Pulsar 是否运行
docker ps | grep pulsar

# 检查 Pulsar 健康状态
docker exec pulsar-standalone bin/pulsar-admin brokers healthcheck

# 查看 Pulsar 日志
docker logs pulsar-standalone
```

## 📖 相关文档

- [Consumer ACK 机制详解](../../../Pulsar/01-入门篇/07-Consumer-ACK机制.md)
- [Pulsar Consumer 官方文档](https://pulsar.apache.org/docs/concepts-messaging/#consumers)
- [快速入门示例](../quickstart/)
- [Producer 发送模式](../producer-modes/)
- [订阅模式示例](../subscription-modes/)

## 🔗 完整代码

[查看完整代码示例](https://github.com/pingxin403/apache-pulsar-examples/tree/main/java-examples/consumer-ack)
