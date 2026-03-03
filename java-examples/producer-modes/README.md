# Producer 发送模式示例

本示例演示 Apache Pulsar Producer 的四种核心发送模式：同步发送、异步发送、批量发送和带 Key 路由。

## 📋 功能说明

Producer 发送模式是 Pulsar 消息发送的核心概念，不同的发送模式适用于不同的业务场景：

- **同步发送**: 可靠性优先，适合关键业务数据
- **异步发送**: 吞吐量优先，适合高并发场景
- **批量发送**: 效率优先，适合大量小消息
- **带 Key 路由**: 顺序性优先，适合需要保序的场景

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

#### 同步发送示例
```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.SyncProducerExample"
```

#### 异步发送示例
```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.AsyncProducerExample"
```

#### 批量发送示例
```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.BatchProducerExample"
```

#### 带 Key 路由示例
```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.KeyBasedProducerExample"
```

## 📚 示例详解

### 1. 同步发送 (`SyncProducerExample.java`)

**核心代码**：
```java
// 同步发送，阻塞等待响应
MessageId msgId = producer.send("订单 12345 支付成功");
logger.info("✅ 消息发送成功，MessageId: " + msgId);
```

**特点**：
- ✅ 可靠性高：立即知道发送结果
- ✅ 简单易用：代码逻辑清晰
- ❌ 吞吐量低：每次发送都需要等待响应
- ❌ 延迟高：阻塞主线程

**适用场景**：
- 支付订单确认
- 重要通知发送
- 关键业务数据
- 需要立即确认发送结果的场景

**预期输出**：
```
开始同步发送消息...
✅ 消息发送成功，MessageId: 1:0:-1:0
程序执行完成
```

### 2. 异步发送 (`AsyncProducerExample.java`)

**核心代码**：
```java
// 异步发送，不阻塞主线程
producer.sendAsync("用户点击事件 " + i)
    .thenAccept(msgId -> {
        logger.info("✅ 消息发送成功: " + msgId);
    })
    .exceptionally(ex -> {
        logger.log(Level.WARNING, "❌ 消息发送失败", ex);
        return null;
    });

// 等待所有异步消息发送完成
producer.flush();
```

**特点**：
- ✅ 吞吐量高：不阻塞主线程
- ✅ 性能好：可以并发发送多条消息
- ❌ 复杂度高：需要处理回调
- ❌ 错误处理复杂：需要在回调中处理异常

**适用场景**：
- 用户行为日志
- 实时监控数据
- 高并发场景
- 对延迟不敏感的场景

**预期输出**：
```
开始异步发送消息...
✅ 消息 0 发送成功: 1:0:-1:0
✅ 消息 100 发送成功: 1:0:-1:100
...
发送完成 - 成功: 1000, 失败: 0
程序执行完成
```

### 3. 批量发送 (`BatchProducerExample.java`)

**核心代码**：
```java
Producer<String> producer = client.newProducer(Schema.STRING)
    .topic("persistent://public/default/iot-sensor-data")
    .enableBatching(true)  // 启用批量发送
    .batchingMaxMessages(100)  // 每批最多 100 条消息
    .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)  // 最多等待 10ms
    .create();

// 发送消息（会自动批量）
for (int i = 0; i < 1000; i++) {
    producer.sendAsync("传感器数据 " + i);
}

// 等待所有批次发送完成
producer.flush();
```

**特点**：
- ✅ 高效率：降低网络开销
- ✅ 高吞吐：适合大量小消息
- ❌ 增加延迟：需要等待批次填满或超时
- ❌ 内存占用：需要缓存消息

**配置参数**：
- `enableBatching(true)`: 启用批量发送
- `batchingMaxMessages(100)`: 每批最多消息数
- `batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)`: 最大等待时间

**适用场景**：
- IoT 设备数据上报
- 日志收集系统
- 监控指标上报
- 大量小消息场景

**预期输出**：
```
开始批量发送消息...
批量配置 - 最大消息数: 100, 最大延迟: 10ms
已发送 0 条消息
已发送 200 条消息
...
✅ 所有消息已发送完成
程序执行完成
```

### 4. 带 Key 路由 (`KeyBasedProducerExample.java`)

**核心代码**：
```java
// 使用 Key 路由消息
MessageId msgId = producer.newMessage()
    .key("user-12345")  // 关键：指定 Key
    .value("用户 12345 创建订单")
    .send();

// 相同 Key 的消息会路由到同一个分区
producer.newMessage()
    .key("user-12345")
    .value("用户 12345 支付订单")
    .send();
```

**特点**：
- ✅ 保证顺序：相同 Key 的消息发送到同一分区
- ✅ 并行处理：不同 Key 可以并行消费
- ❌ 负载不均：可能导致分区负载不均衡
- ❌ 热点问题：热门 Key 可能成为瓶颈

**路由规则**：
```
partition_index = hash(message_key) % num_partitions
```

**适用场景**：
- 用户行为分析（按用户 ID 分组）
- 订单状态机（按订单 ID 分组）
- IoT 设备数据（按设备 ID 分组）
- 需要保证消息顺序的场景

**预期输出**：
```
开始发送带 Key 的消息...
✅ 发送消息 1: 1:0:-1:0
✅ 发送消息 2: 1:0:-1:1
✅ 发送消息 3: 1:0:-1:2
✅ 发送消息 4: 1:1:-1:0
✅ 所有订单事件已发送完成
程序执行完成
```

## 🔍 发送模式对比

| 特性 | 同步发送 | 异步发送 | 批量发送 | 带 Key 路由 |
|------|---------|---------|---------|------------|
| 可靠性 | ✅ 高 | ✅ 高 | ✅ 高 | ✅ 高 |
| 吞吐量 | ❌ 低 | ✅ 高 | ✅ 高 | ✅ 中 |
| 延迟 | ❌ 高 | ✅ 低 | ❌ 中 | ✅ 低 |
| 顺序性 | ✅ 是 | ❌ 否 | ❌ 否 | ✅ 按 Key |
| 复杂度 | ✅ 简单 | ❌ 复杂 | ❌ 中等 | ✅ 简单 |
| 适用场景 | 关键业务 | 高并发 | 大量小消息 | 需要保序 |

## 💡 最佳实践

### 1. 选择合适的发送模式

- **关键业务数据**：使用同步发送，确保可靠性
- **高并发场景**：使用异步发送，提高吞吐量
- **大量小消息**：使用批量发送，降低网络开销
- **需要保序**：使用带 Key 路由，保证顺序性

### 2. 异常处理

所有示例都包含完整的异常处理：
```java
try (PulsarClient client = ...; Producer<String> producer = ...) {
    // 发送消息
} catch (PulsarClientException e) {
    logger.log(Level.SEVERE, "❌ Pulsar 客户端错误", e);
    System.exit(1);
}
```

### 3. 资源管理

使用 try-with-resources 确保资源自动释放：
```java
try (PulsarClient client = ...;
     Producer<String> producer = ...) {
    // 使用 client 和 producer
}
// 自动关闭资源
```

### 4. 批量发送配置建议

```java
.enableBatching(true)
.batchingMaxMessages(100)           // 根据消息大小调整
.batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)  // 根据延迟要求调整
.batchingMaxBytes(128 * 1024)       // 128KB，防止批次过大
```

### 5. Key 选择建议

- 选择分布均匀的 Key（如用户 ID、订单 ID）
- 避免使用热点 Key（如固定值、时间戳）
- 考虑使用复合 Key（如 "region:userId"）

## 🔧 故障排查

### 连接失败

```bash
# 检查 Pulsar 是否运行
docker ps | grep pulsar

# 检查 Pulsar 健康状态
docker exec pulsar-standalone bin/pulsar-admin brokers healthcheck
```

### 编译失败

```bash
# 检查 Java 版本
java -version  # 应该是 11+

# 检查 Maven 版本
mvn -version   # 应该是 3.6+

# 清理并重新编译
mvn clean package
```

### 消息发送失败

- 检查 Topic 是否存在
- 检查网络连接
- 查看 Pulsar 日志：`docker logs pulsar-standalone`

## 📖 相关文档

- [Producer 发送模式全解析](../../../Pulsar/01-入门篇/06-Producer发送模式.md)
- [Pulsar Producer 官方文档](https://pulsar.apache.org/docs/concepts-messaging/#producers)
- [快速入门示例](../quickstart/)
- [Consumer ACK 机制](../consumer-ack/)

## 🔗 完整代码

[查看完整代码示例](https://github.com/pingxin403/apache-pulsar-examples/tree/main/java-examples/producer-modes)
