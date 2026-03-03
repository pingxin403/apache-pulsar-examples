# Pulsar Dead Letter Topic 示例

本示例演示如何在 Apache Pulsar 中使用死信队列（Dead Letter Topic, DLT）机制，处理无法正常消费的消息。

## 📋 功能说明

Dead Letter Topic 是 Pulsar 的重要特性，用于处理失败的消息：

- **自动重试**: 消息处理失败后自动重新投递
- **重试次数限制**: 达到最大重试次数后进入死信队列
- **隔离失败消息**: 避免失败消息阻塞正常消息处理
- **人工介入**: 可以单独处理死信队列中的消息

## 🚀 快速开始

### 1. 启动 Pulsar

```bash
cd ../../docker-compose
docker-compose up -d
```

### 2. 编译项目

```bash
mvn clean package
```

### 3. 运行示例

```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.DeadLetterTopicExample"
```

## 📚 示例说明

### 配置 Dead Letter Policy

```java
Consumer<String> consumer = client.newConsumer(Schema.STRING)
        .topic(TOPIC)
        .subscriptionName("dlt-subscription")
        .negativeAckRedeliveryDelay(1, TimeUnit.SECONDS)  // 重试延迟
        .deadLetterPolicy(DeadLetterPolicy.builder()
                .maxRedeliverCount(3)  // 最多重试 3 次
                .deadLetterTopic(DLT_TOPIC)  // 死信队列 Topic
                .build())
        .subscribe();
```

### 处理失败消息

```java
try {
    // 处理消息
    processMessage(message);
    consumer.acknowledge(message);
} catch (Exception e) {
    // 处理失败，发送 Negative ACK
    consumer.negativeAcknowledge(message);
    // 消息会自动重试，达到最大次数后进入死信队列
}
```

### 处理死信消息

```java
// 创建 DLT Consumer
Consumer<String> dltConsumer = client.newConsumer(Schema.STRING)
        .topic(DLT_TOPIC)
        .subscriptionName("dlt-consumer")
        .subscribe();

// 接收死信消息
Message<String> dltMessage = dltConsumer.receive();

// 特殊处理：记录日志、发送告警、人工介入等
handleDeadLetterMessage(dltMessage);

dltConsumer.acknowledge(dltMessage);
```

## 🔍 执行流程

```
正常消息流程:
Producer → Topic → Consumer → 处理成功 → ACK

失败消息流程:
Producer → Topic → Consumer → 处理失败 → Negative ACK
                      ↓
                   重试 1 次 → 处理失败 → Negative ACK
                      ↓
                   重试 2 次 → 处理失败 → Negative ACK
                      ↓
                   重试 3 次 → 处理失败 → Negative ACK
                      ↓
                 Dead Letter Topic → DLT Consumer → 特殊处理
```

## 💡 使用场景

### 1. 消息格式错误

```java
try {
    User user = parseMessage(message.getValue());
    processUser(user);
    consumer.acknowledge(message);
} catch (JsonParseException e) {
    // 格式错误，无法解析，进入死信队列
    consumer.negativeAcknowledge(message);
}
```

### 2. 外部服务不可用

```java
try {
    callExternalAPI(message.getValue());
    consumer.acknowledge(message);
} catch (ServiceUnavailableException e) {
    // 外部服务不可用，重试
    consumer.negativeAcknowledge(message);
}
```

### 3. 业务逻辑错误

```java
try {
    validateBusinessRule(message.getValue());
    consumer.acknowledge(message);
} catch (BusinessRuleException e) {
    // 业务规则验证失败，进入死信队列
    consumer.negativeAcknowledge(message);
}
```

### 4. 死信消息处理

```java
// 监控死信队列
Consumer<String> dltConsumer = client.newConsumer(Schema.STRING)
        .topic(DLT_TOPIC)
        .subscribe();

while (true) {
    Message<String> dltMessage = dltConsumer.receive();
    
    // 1. 记录到日志系统
    logger.error("Dead letter message: {}", dltMessage.getValue());
    
    // 2. 发送告警
    alertService.sendAlert("DLT message detected", dltMessage);
    
    // 3. 存储到数据库，等待人工处理
    database.save(dltMessage);
    
    // 4. 尝试修复后重新发送
    if (canBeFixed(dltMessage)) {
        String fixed = fixMessage(dltMessage);
        producer.send(fixed);
    }
    
    dltConsumer.acknowledge(dltMessage);
}
```

## ⚙️ 配置参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| maxRedeliverCount | 最大重试次数 | 无限制 |
| deadLetterTopic | 死信队列 Topic 名称 | {原Topic}-DLQ |
| negativeAckRedeliveryDelay | 重试延迟时间 | 1 分钟 |
| retryLetterTopic | 重试队列 Topic 名称（可选） | {原Topic}-RETRY |

## ⚠️ 注意事项

1. **重试次数**: 根据业务场景合理设置，避免过多重试
2. **重试延迟**: 设置合理的延迟时间，避免频繁重试
3. **死信监控**: 及时监控死信队列，处理异常消息
4. **资源占用**: 死信队列也会占用存储空间，需要定期清理

## 🆚 与其他方案对比

| 方案 | 优点 | 缺点 |
|------|------|------|
| Dead Letter Topic | 自动化，无需额外代码 | 配置相对复杂 |
| 手动重试 | 灵活控制 | 需要编写重试逻辑 |
| 异常队列 | 简单直接 | 需要额外的队列管理 |
| 忽略失败 | 简单 | 可能丢失重要消息 |

## 📖 相关文档

- [Pulsar Dead Letter Topic 官方文档](https://pulsar.apache.org/docs/concepts-messaging/#dead-letter-topic)
- [技术文章：Dead Letter Topic机制](../../Pulsar/03-核心功能深度篇/07-Dead-Letter-Topic机制.md)

## 🔗 完整代码

[查看完整代码示例](https://github.com/pingxin403/apache-pulsar-examples/tree/main/java-examples/dead-letter-topic)
