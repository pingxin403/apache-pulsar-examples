# Pulsar 延迟消息示例

本示例演示如何在 Apache Pulsar 中使用延迟消息（Delayed Message）功能，实现定时任务和延迟处理。

## 📋 功能说明

延迟消息允许你指定消息在未来某个时间点才被消费者接收，适用于：

- **定时任务**: 定时发送通知、提醒
- **延迟处理**: 订单超时取消、支付超时关闭
- **重试机制**: 失败后延迟重试
- **限流控制**: 控制消息处理速率

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
mvn exec:java -Dexec.mainClass="com.example.pulsar.DelayedMessageExample"
```

## 📚 示例说明

### 延迟消息发送

使用 `deliverAfter()` 方法指定延迟时间：

```java
// 延迟 5 秒
producer.newMessage()
        .value("延迟 5 秒的消息")
        .deliverAfter(5, TimeUnit.SECONDS)
        .send();

// 延迟 10 秒
producer.newMessage()
        .value("延迟 10 秒的消息")
        .deliverAfter(10, TimeUnit.SECONDS)
        .send();

// 立即发送（不延迟）
producer.newMessage()
        .value("立即发送的消息")
        .send();
```

### 支持的时间单位

- `TimeUnit.SECONDS` - 秒
- `TimeUnit.MINUTES` - 分钟
- `TimeUnit.HOURS` - 小时
- `TimeUnit.DAYS` - 天

## 🔍 执行流程

```
时间轴:
00:00 ─┬─ 发送延迟 5 秒的消息
       ├─ 发送延迟 10 秒的消息
       ├─ 发送延迟 15 秒的消息
       └─ 发送立即消息
       
00:00 ─── 立即接收到立即消息
00:05 ─── 接收到延迟 5 秒的消息
00:10 ─── 接收到延迟 10 秒的消息
00:15 ─── 接收到延迟 15 秒的消息
```

## 💡 使用场景

### 1. 订单超时取消

```java
// 创建订单后，发送 30 分钟延迟消息
producer.newMessage()
        .value("订单ID: " + orderId)
        .deliverAfter(30, TimeUnit.MINUTES)
        .send();

// Consumer 接收到消息后，检查订单状态
// 如果未支付，则自动取消订单
```

### 2. 定时提醒

```java
// 会议前 15 分钟发送提醒
long delayMinutes = calculateDelayMinutes(meetingTime);
producer.newMessage()
        .value("会议提醒: " + meetingTitle)
        .deliverAfter(delayMinutes, TimeUnit.MINUTES)
        .send();
```

### 3. 失败重试

```java
// 处理失败后，延迟 1 分钟重试
if (processFailed) {
    producer.newMessage()
            .value("重试任务: " + taskId)
            .deliverAfter(1, TimeUnit.MINUTES)
            .send();
}
```

### 4. 限流控制

```java
// 每条消息延迟 100 毫秒，控制处理速率
for (String message : messages) {
    producer.newMessage()
            .value(message)
            .deliverAfter(100, TimeUnit.MILLISECONDS)
            .send();
}
```

## ⚠️ 注意事项

1. **延迟精度**: 延迟时间是近似值，实际延迟可能略有偏差
2. **最大延迟**: 建议不超过 1 天，超长延迟可能影响性能
3. **顺序保证**: 延迟消息不保证严格的顺序
4. **资源占用**: 大量延迟消息会占用 Broker 内存

## 🆚 与其他方案对比

| 方案 | 优点 | 缺点 |
|------|------|------|
| Pulsar 延迟消息 | 简单易用，无需额外组件 | 延迟精度较低 |
| 定时任务（Cron） | 精确控制执行时间 | 需要额外的调度系统 |
| 延迟队列（Redis） | 高性能，精度高 | 需要额外的 Redis 集群 |
| 消息重投递 | 灵活控制 | 实现复杂 |

## 📖 相关文档

- [Pulsar Delayed Message 官方文档](https://pulsar.apache.org/docs/concepts-messaging/#delayed-message-delivery)
- [技术文章：延迟消息实战](../../Pulsar/03-核心功能深度篇/09-延迟消息实战.md)

## 🔗 完整代码

[查看完整代码示例](https://github.com/pingxin403/apache-pulsar-examples/tree/main/java-examples/delayed-messages)
