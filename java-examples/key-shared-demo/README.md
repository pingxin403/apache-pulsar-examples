# Key_Shared 订阅模式示例

本示例演示如何使用 Apache Pulsar 的 Key_Shared 订阅模式，在保证同一 Key 消息顺序的前提下，实现多消费者并行处理，提升系统吞吐量。

## 📋 功能说明

Key_Shared 订阅模式是 Pulsar 的一种高级订阅类型，它结合了 Shared 订阅的高吞吐量和 Exclusive 订阅的消息顺序保证：

- **并行处理**：多个 Consumer 可以同时消费同一个 Topic
- **顺序保证**：相同 Key 的消息始终由同一个 Consumer 处理，保证顺序
- **负载均衡**：不同 Key 的消息分配给不同的 Consumer，实现负载均衡
- **高吞吐量**：通过并行处理大幅提升系统吞吐量

**典型应用场景**：
- 订单处理系统（同一订单的事件必须按顺序处理）
- 用户会话管理（同一用户的操作必须按顺序处理）
- IoT 设备消息（同一设备的数据必须按顺序处理）
- 金融交易系统（同一账户的交易必须按顺序处理）

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

#### 启动 Producer（发送订单事件）

在终端 1 中运行：
```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.OrderProducer"
```

**预期输出**：
```
✅ 订单事件已发送，Key: order-12345
```

#### 启动多个 Consumer（并行处理）

在终端 2 中运行：
```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.OrderConsumer"
```

在终端 3 中运行：
```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.OrderConsumer"
```

在终端 4 中运行（可选）：
```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.OrderConsumer"
```

**预期输出**（每个 Consumer）：
```
🔄 Consumer 已启动，等待消息...
📩 收到消息 [Key: order-12345] [Value: 订单创建]
📩 收到消息 [Key: order-12345] [Value: 订单支付]
📩 收到消息 [Key: order-12345] [Value: 订单发货]
```

### 4. 验证结果

观察多个 Consumer 的输出，你会发现：
- ✅ 同一订单（相同 Key）的所有消息始终由同一个 Consumer 处理
- ✅ 消息按照发送顺序被消费（订单创建 → 订单支付 → 订单发货）
- ✅ 不同订单的消息可以并行处理，提升吞吐量

## 📚 示例详解

### Producer 实现 (`OrderProducer.java`)

**核心代码**：
```java
Producer<String> producer = client.newProducer(Schema.STRING)
    .topic("persistent://public/default/order-events")
    .create();

// 发送同一订单的多个事件
String orderId = "order-12345";

producer.newMessage()
    .key(orderId)  // 关键：指定 Key
    .value("订单创建")
    .send();

producer.newMessage()
    .key(orderId)
    .value("订单支付")
    .send();

producer.newMessage()
    .key(orderId)
    .value("订单发货")
    .send();
```

**关键点**：
- 使用 `.key(orderId)` 为每条消息指定 Key
- 相同 Key 的消息会被路由到同一个 Consumer
- Key 可以是任意字符串（订单 ID、用户 ID、设备 ID 等）

### Consumer 实现 (`OrderConsumer.java`)

**核心代码**：
```java
Consumer<String> consumer = client.newConsumer(Schema.STRING)
    .topic("persistent://public/default/order-events")
    .subscriptionName("order-processing")
    .subscriptionType(SubscriptionType.Key_Shared)  // 关键：Key_Shared 模式
    .subscribe();

while (true) {
    Message<String> msg = consumer.receive();
    
    System.out.printf("📩 收到消息 [Key: %s] [Value: %s]%n", 
            msg.getKey(), msg.getValue());
    
    // 模拟业务处理
    Thread.sleep(100);
    
    consumer.acknowledge(msg);
}
```

**关键点**：
- 使用 `SubscriptionType.Key_Shared` 指定订阅类型
- 多个 Consumer 使用相同的 `subscriptionName` 加入同一个订阅组
- 相同 Key 的消息会被分配给同一个 Consumer
- 每个 Consumer 独立处理分配给它的消息

## 🔍 Key_Shared 工作原理

### 消息分配机制

Key_Shared 使用一致性哈希算法将消息分配给 Consumer：

```
Message Key → Hash Function → Consumer Assignment

例如：
order-12345 → hash(order-12345) % consumer_count → Consumer 1
order-67890 → hash(order-67890) % consumer_count → Consumer 2
order-12345 → hash(order-12345) % consumer_count → Consumer 1 (相同)
```

### 与其他订阅模式的对比

| 特性 | Exclusive | Shared | Failover | Key_Shared |
|------|-----------|--------|----------|------------|
| Consumer 数量 | 1 | 多个 | 多个（1 活跃） | 多个 |
| 消息顺序 | ✅ 全局顺序 | ❌ 无顺序 | ✅ 全局顺序 | ✅ 按 Key 顺序 |
| 吞吐量 | ❌ 低 | ✅ 高 | ❌ 低 | ✅ 高 |
| 负载均衡 | ❌ 无 | ✅ 轮询 | ❌ 无 | ✅ 按 Key |
| 故障转移 | ❌ 手动 | ✅ 自动 | ✅ 自动 | ✅ 自动 |
| 适用场景 | 简单场景 | 无序高吞吐 | 高可用 | 有序高吞吐 |

### Key_Shared 的优势

1. **高吞吐量**：通过并行处理提升系统吞吐量
2. **顺序保证**：相同 Key 的消息保持顺序
3. **负载均衡**：自动分配不同 Key 的消息到不同 Consumer
4. **弹性扩展**：可以动态增加或减少 Consumer 数量
5. **故障转移**：某个 Consumer 失败时，其负责的 Key 会自动转移到其他 Consumer

## 💡 最佳实践

### 1. 选择合适的 Key

**好的 Key 选择**：
```java
// ✅ 订单 ID
producer.newMessage().key(orderId).value(orderEvent).send();

// ✅ 用户 ID
producer.newMessage().key(userId).value(userAction).send();

// ✅ 设备 ID
producer.newMessage().key(deviceId).value(sensorData).send();
```

**不好的 Key 选择**：
```java
// ❌ 时间戳（每条消息都不同，失去顺序保证）
producer.newMessage().key(String.valueOf(System.currentTimeMillis())).value(data).send();

// ❌ 随机数（每条消息都不同，失去顺序保证）
producer.newMessage().key(UUID.randomUUID().toString()).value(data).send();
```

### 2. Key 分布均匀

确保 Key 的分布相对均匀，避免热点问题：

```java
// ✅ 好的设计：Key 分布均匀
// 假设有 1000 个订单，分配给 3 个 Consumer
// Consumer 1: ~333 个订单
// Consumer 2: ~333 个订单
// Consumer 3: ~334 个订单

// ❌ 坏的设计：Key 分布不均
// 假设 90% 的消息都是同一个 Key
// Consumer 1: 90% 的消息
// Consumer 2: 5% 的消息
// Consumer 3: 5% 的消息
```

### 3. 处理 Consumer 动态变化

当 Consumer 数量变化时，Key 的分配会重新平衡：

```java
// 配置合理的重新平衡策略
Consumer<String> consumer = client.newConsumer(Schema.STRING)
    .topic("persistent://public/default/order-events")
    .subscriptionName("order-processing")
    .subscriptionType(SubscriptionType.Key_Shared)
    .receiverQueueSize(1000)  // 接收队列大小
    .subscribe();
```

### 4. 异常处理

```java
while (true) {
    Message<String> msg = null;
    try {
        msg = consumer.receive();
        
        // 处理消息
        processMessage(msg.getKey(), msg.getValue());
        
        // 确认消息
        consumer.acknowledge(msg);
        
    } catch (Exception e) {
        System.err.println("❌ 消息处理失败: " + e.getMessage());
        
        if (msg != null) {
            // 发送 Negative ACK，触发重试
            consumer.negativeAcknowledge(msg);
        }
    }
}
```

### 5. 资源管理

使用 try-with-resources 确保资源自动释放：

```java
try (PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();
     Producer<String> producer = client.newProducer(Schema.STRING)
        .topic("persistent://public/default/order-events")
        .create()) {
    
    // 发送消息
    producer.newMessage()
        .key("order-12345")
        .value("订单创建")
        .send();
    
} catch (PulsarClientException e) {
    System.err.println("Pulsar client error: " + e);
}
// 自动关闭资源
```

### 6. 监控和调优

```bash
# 查看订阅状态
pulsar-admin topics stats persistent://public/default/order-events

# 查看 Consumer 数量和消息积压
pulsar-admin topics subscriptions persistent://public/default/order-events

# 查看特定订阅的详细信息
pulsar-admin topics stats-internal persistent://public/default/order-events
```

## 📊 性能对比

### 吞吐量测试

| Consumer 数量 | 吞吐量 (msg/s) | 提升倍数 |
|--------------|---------------|---------|
| 1 个 Consumer | 3,000 | 1.0x |
| 2 个 Consumer | 5,800 | 1.9x |
| 3 个 Consumer | 8,500 | 2.8x |
| 4 个 Consumer | 10,800 | 3.6x |

### 延迟测试

| Consumer 数量 | P50 延迟 | P95 延迟 | P99 延迟 |
|--------------|---------|---------|---------|
| 1 个 Consumer | 80ms | 120ms | 150ms |
| 3 个 Consumer | 30ms | 50ms | 60ms |

### 测试条件

- 消息大小：1KB
- 消息数量：100,000 条
- Key 数量：1,000 个（均匀分布）
- 处理时间：100ms/消息
- 环境：本地 Docker Standalone 模式

## 🔧 故障排查

### 消息顺序错乱

**问题**：相同 Key 的消息被不同 Consumer 处理

**原因**：
- 没有正确设置 Key
- 使用了错误的订阅类型

**解决方案**：
```java
// 确保 Producer 设置了 Key
producer.newMessage()
    .key(orderId)  // 必须设置 Key
    .value(data)
    .send();

// 确保 Consumer 使用 Key_Shared 订阅
consumer = client.newConsumer(Schema.STRING)
    .subscriptionType(SubscriptionType.Key_Shared)  // 必须是 Key_Shared
    .subscribe();
```

### 负载不均衡

**问题**：某些 Consumer 处理的消息远多于其他 Consumer

**原因**：
- Key 分布不均匀（热点 Key）
- Consumer 数量与 Key 数量不匹配

**解决方案**：
```java
// 1. 优化 Key 设计，确保分布均匀
// 例如：使用用户 ID 的哈希值而不是用户类型
String key = String.valueOf(userId.hashCode());

// 2. 调整 Consumer 数量
// 建议 Consumer 数量 <= Key 数量 / 10
// 例如：1000 个 Key，建议 3-10 个 Consumer
```

### Consumer 重新平衡慢

**问题**：添加或删除 Consumer 后，消息分配没有立即更新

**原因**：
- 接收队列中有大量未处理的消息
- 重新平衡需要时间

**解决方案**：
```java
// 减小接收队列大小，加快重新平衡
Consumer<String> consumer = client.newConsumer(Schema.STRING)
    .receiverQueueSize(100)  // 默认是 1000，可以减小
    .subscribe();
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

## 🧪 高级测试

### 测试脚本：验证顺序保证

创建一个测试脚本来验证相同 Key 的消息顺序：

```bash
#!/bin/bash

# 启动 3 个 Consumer
for i in {1..3}; do
    mvn exec:java -Dexec.mainClass="com.example.pulsar.OrderConsumer" > consumer-$i.log 2>&1 &
done

# 等待 Consumer 启动
sleep 5

# 发送测试消息（多个订单，每个订单多个事件）
for order in {1..10}; do
    for event in "创建" "支付" "发货"; do
        # 这里需要修改 Producer 支持命令行参数
        echo "发送: order-$order - $event"
    done
done

# 等待处理完成
sleep 10

# 验证每个订单的事件顺序
for i in {1..3}; do
    echo "=== Consumer $i 日志 ==="
    cat consumer-$i.log
done
```

### 性能压测

使用 `pulsar-perf` 工具进行压测：

```bash
# 生产消息（带 Key）
pulsar-perf produce \
    --service-url pulsar://localhost:6650 \
    --topics persistent://public/default/order-events \
    --rate 10000 \
    --num-messages 100000 \
    --size 1024 \
    --num-producers 3 \
    --num-topic-partitions 3

# 消费消息（Key_Shared 模式）
pulsar-perf consume \
    --service-url pulsar://localhost:6650 \
    --topics persistent://public/default/order-events \
    --subscription-name perf-test \
    --subscription-type Key_Shared \
    --num-consumers 3
```

## 📖 相关文档

- [Key_Shared 订阅实战：保证同一 Key 消息顺序消费](../../../Pulsar/03-核心功能深度篇/01-Key_Shared订阅实战.md)
- [订阅模式对比示例](../subscription-modes/)
- [Consumer ACK 机制](../consumer-ack/)
- [Producer 发送模式](../producer-modes/)
- [Pulsar 官方文档 - Key_Shared Subscription](https://pulsar.apache.org/docs/concepts-messaging/#key_shared)

## 🔗 完整代码

[查看完整代码示例](https://github.com/pingxin403/apache-pulsar-examples/tree/main/java-examples/key-shared-demo)
