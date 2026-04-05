# 金融支付系统 - Pulsar 事务应用

基于 Pulsar 事务的跨账户转账系统，演示 Exactly-Once 语义在金融场景中的应用。

## 📋 架构

```
PaymentService
  ├─ [debit-events]       扣款事件
  ├─ [credit-events]      入账事件
  └─ [transaction-log]    交易日志

三个 Topic 的写入在同一个 Pulsar Transaction 中完成，保证原子性。
```

## 🚀 快速开始

### 1. 启动 Pulsar（开启事务支持）

```bash
docker-compose up -d
```

### 2. 编译运行

```bash
mvn clean package -q
mvn exec:java -Dexec.mainClass="com.example.pulsar.PaymentService"
```

### 3. 查看结果

```bash
# 查看扣款事件
docker exec pulsar-payment bin/pulsar-client consume debit-events -s viewer -n 0

# 查看入账事件
docker exec pulsar-payment bin/pulsar-client consume credit-events -s viewer -n 0

# 查看交易日志
docker exec pulsar-payment bin/pulsar-client consume transaction-log -s viewer -n 0
```

## 📖 相关文章

- [金融支付系统中的 Pulsar 事务应用](../../../Pulsar/08-实战项目篇/02-金融支付系统中的Pulsar事务应用.md)
- [Exactly-Once 语义实现](../../../Pulsar/03-核心功能深度篇/02-Exactly-Once语义实现.md)
