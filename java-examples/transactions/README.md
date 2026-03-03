# Pulsar 事务消息示例

本示例演示如何在 Apache Pulsar 中使用事务（Transaction）功能，确保消息的原子性和一致性。

## 📋 功能说明

Pulsar 事务提供以下保证：

- **原子性**: 事务中的所有操作要么全部成功，要么全部失败
- **一致性**: 确保消息的发送和确认在同一个事务中
- **隔离性**: 未提交的事务对其他消费者不可见
- **持久性**: 提交的事务会被持久化

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

#### 先运行 Producer（发送事务消息）
```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.TransactionProducerExample"
```

#### 再运行 Consumer（消费事务消息）
```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.TransactionConsumerExample"
```

## 📚 示例说明

### 1. 事务 Producer (`TransactionProducerExample.java`)

演示两种场景：

**场景 1: 成功提交事务**
```java
Transaction txn = client.newTransaction()
        .withTransactionTimeout(1, TimeUnit.MINUTES)
        .build()
        .get();

try {
    // 在事务中发送多条消息
    producer.newMessage(txn).value("Message 1").send();
    producer.newMessage(txn).value("Message 2").send();
    producer.newMessage(txn).value("Message 3").send();
    
    // 提交事务
    txn.commit().get();
} catch (Exception e) {
    // 回滚事务
    txn.abort().get();
}
```

**场景 2: 回滚事务**
- 模拟业务错误
- 自动回滚事务
- 消息不会被消费者看到

### 2. 事务 Consumer (`TransactionConsumerExample.java`)

在事务中消费和确认消息：

```java
Transaction txn = client.newTransaction()
        .withTransactionTimeout(1, TimeUnit.MINUTES)
        .build()
        .get();

try {
    // 在事务中确认消息
    consumer.acknowledgeAsync(message.getMessageId(), txn).get();
    
    // 处理业务逻辑
    processMessage(message.getValue());
    
    // 提交事务
    txn.commit().get();
} catch (Exception e) {
    // 回滚事务，消息将被重新投递
    txn.abort().get();
}
```

## 🔍 执行流程

```
Producer                    Broker                     Consumer
   |                          |                           |
   |--1. 开始事务------------->|                           |
   |                          |                           |
   |--2. 发送消息 1---------->|                           |
   |--3. 发送消息 2---------->|                           |
   |--4. 发送消息 3---------->|                           |
   |                          |                           |
   |--5. 提交事务------------>|                           |
   |                          |                           |
   |                          |<--6. 接收消息-------------|
   |                          |                           |
   |                          |<--7. 事务确认-------------|
   |                          |                           |
```

## 💡 使用场景

1. **金融支付系统**
   - 确保支付消息和账户更新的原子性
   - 避免重复扣款或漏扣款

2. **订单处理系统**
   - 订单创建、库存扣减、支付确认在同一事务中
   - 任何环节失败都会回滚

3. **数据同步系统**
   - 确保多个数据源的一致性
   - 避免部分更新导致的数据不一致

## ⚠️ 注意事项

1. **启用事务**: 客户端必须设置 `enableTransaction(true)`
2. **超时设置**: Producer 的 `sendTimeout` 必须设置为 0
3. **事务超时**: 默认 1 分钟，可以根据业务需求调整
4. **性能影响**: 事务会增加延迟，不适合高吞吐量场景

## 📖 相关文档

- [Pulsar Transactions 官方文档](https://pulsar.apache.org/docs/transactions/)
- [技术文章：Exactly-Once语义实现](../../Pulsar/03-核心功能深度篇/02-Exactly-Once语义实现.md)
- [技术文章：金融支付系统中的Pulsar事务应用](../../Pulsar/08-实战项目篇/02-金融支付系统中的Pulsar事务应用.md)

## 🔗 完整代码

[查看完整代码示例](https://github.com/pingxin403/apache-pulsar-examples/tree/main/java-examples/transactions)
