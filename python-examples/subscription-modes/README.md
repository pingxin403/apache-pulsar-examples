# Pulsar 订阅模式示例 (Python)

本示例演示 Apache Pulsar 的四种订阅模式：Exclusive、Shared、Failover 和 Key_Shared。

## 📋 功能说明

Pulsar 提供四种订阅模式，满足不同的业务场景：

- **Exclusive**: 独占模式，只有一个 Consumer 可以消费
- **Shared**: 共享模式，多个 Consumer 共享消息，实现负载均衡
- **Failover**: 故障转移模式，主 Consumer 故障时自动切换到备用
- **Key_Shared**: 按 Key 共享模式，相同 Key 的消息保证顺序

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 启动 Pulsar

```bash
cd ../../docker-compose
docker-compose up -d
```

### 3. 运行示例

#### Exclusive 订阅模式
```bash
python exclusive_subscription.py
```

#### Shared 订阅模式
```bash
python shared_subscription.py
```

#### Failover 订阅模式
```bash
python failover_subscription.py
```

#### Key_Shared 订阅模式
```bash
python key_shared_subscription.py
```

## 📚 示例说明

### 1. Exclusive 订阅模式

```python
consumer = client.subscribe(
    TOPIC,
    subscription_name='exclusive-subscription',
    consumer_type=pulsar.ConsumerType.Exclusive
)
```

**特点**：
- 只有一个 Consumer 可以消费
- 保证消息顺序
- 适合需要严格顺序的场景

### 2. Shared 订阅模式

```python
consumer = client.subscribe(
    TOPIC,
    subscription_name='shared-subscription',
    consumer_type=pulsar.ConsumerType.Shared
)
```

**特点**：
- 多个 Consumer 共享消息
- 实现负载均衡
- 不保证消息顺序
- 适合高吞吐量场景

### 3. Failover 订阅模式

```python
consumer = client.subscribe(
    TOPIC,
    subscription_name='failover-subscription',
    consumer_type=pulsar.ConsumerType.Failover
)
```

**特点**：
- 主 Consumer 接收消息
- 备用 Consumer 待命
- 主 Consumer 故障时自动切换
- 保证消息顺序
- 适合高可用场景

### 4. Key_Shared 订阅模式

```python
# Producer 发送带 Key 的消息
producer.send(
    message.encode('utf-8'),
    partition_key='user-1'
)

# Consumer 订阅
consumer = client.subscribe(
    TOPIC,
    subscription_name='key-shared-subscription',
    consumer_type=pulsar.ConsumerType.KeyShared
)
```

**特点**：
- 相同 Key 的消息发送到同一个 Consumer
- 保证相同 Key 的消息顺序
- 不同 Key 可并行消费
- 适合需要部分顺序的场景

## 🔍 订阅模式对比

| 特性 | Exclusive | Shared | Failover | Key_Shared |
|------|-----------|--------|----------|------------|
| Consumer 数量 | 1 | 多个 | 多个（1主+N备） | 多个 |
| 消息顺序 | ✅ 保证 | ❌ 不保证 | ✅ 保证 | ⚠️ 部分保证 |
| 负载均衡 | ❌ 无 | ✅ 有 | ❌ 无 | ✅ 有 |
| 高可用 | ❌ 无 | ✅ 有 | ✅ 有 | ✅ 有 |
| 适用场景 | 严格顺序 | 高吞吐量 | 高可用 | 部分顺序 |

## 💡 使用场景

### Exclusive 模式
- 金融交易系统（严格顺序）
- 日志处理（按时间顺序）

### Shared 模式
- 图片处理（无顺序要求）
- 批量数据处理

### Failover 模式
- 关键业务系统（高可用）
- 实时监控系统

### Key_Shared 模式
- 用户行为分析（按用户分组）
- 订单处理（按订单ID分组）

## 📖 相关文档

- [Pulsar Subscription Types 官方文档](https://pulsar.apache.org/docs/concepts-messaging/#subscription-types)
- [技术文章：统一消息模型](../../Pulsar/01-入门篇/04-统一消息模型.md)
- [技术文章：Key_Shared订阅实战](../../Pulsar/03-核心功能深度篇/01-Key_Shared订阅实战.md)

## 🔗 完整代码

[查看完整代码示例](https://github.com/pingxin403/apache-pulsar-examples/tree/main/python-examples/subscription-modes)
