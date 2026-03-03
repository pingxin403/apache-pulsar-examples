# Consumer ACK 机制示例 (Python)

对应文章: `Pulsar/01-入门篇/07-Consumer-ACK机制.md`

## 📋 功能说明

Consumer ACK 机制是 Pulsar 消息消费的核心概念，决定了消息何时被标记为"已消费"。不同的 ACK 机制适用于不同的业务场景：

- **Individual ACK**: 单独确认每条消息，适合需要精确控制的场景
- **Cumulative ACK**: 批量确认消息，适合顺序处理场景
- **Negative ACK**: 明确告知消息处理失败，触发快速重试

## 🚀 快速开始

### 前置条件

1. 安装 Python 3.7+
2. 安装 Pulsar 客户端:
```bash
pip install -r requirements.txt
```
3. 启动 Pulsar 单机容器:
```bash
cd ../../docker-compose
docker-compose up -d
```

验证 Pulsar 是否启动成功：
```bash
docker exec pulsar-standalone bin/pulsar-admin brokers healthcheck
```

### 运行示例

#### 1. Individual ACK 示例
```bash
python individual_ack.py
```

#### 2. Cumulative ACK 示例
```bash
python cumulative_ack.py
```

#### 3. Negative ACK 示例
```bash
python negative_ack.py
```

## 📚 示例详解

### 1. Individual ACK (`individual_ack.py`)

**核心代码**：
```python
consumer = client.subscribe(
    'persistent://public/default/individual-ack-topic',
    'individual-ack-subscription',
    consumer_type=pulsar.ConsumerType.Shared  # Shared 订阅
)

while True:
    msg = consumer.receive()
    
    try:
        process_message(msg.data().decode('utf-8'))
        
        # 单独确认这条消息
        consumer.acknowledge(msg)
        print(f"✅ 消息已确认: {msg.message_id()}")
        
    except Exception as e:
        print(f"❌ 消息处理失败: {e}")
        # 不确认，消息会被重新投递
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

### 2. Cumulative ACK (`cumulative_ack.py`)

**核心代码**：
```python
consumer = client.subscribe(
    'persistent://public/default/cumulative-ack-topic',
    'cumulative-ack-subscription',
    consumer_type=pulsar.ConsumerType.Exclusive  # 必须使用 Exclusive 或 Failover
)

message_count = 0
batch_size = 3

while message_count < 10:
    msg = consumer.receive()
    message_count += 1
    
    process_message(msg.data().decode('utf-8'))
    
    # 每处理 3 条消息，执行一次 Cumulative ACK
    if message_count % batch_size == 0:
        consumer.acknowledge_cumulative(msg)
        print(f"✅ Cumulative ACK: 已确认消息 #1 到 #{message_count}")
```

**特点**：
- ✅ 高效率：减少网络开销（批量确认）
- ✅ 简单易用：适合顺序处理场景
- ❌ 仅限 Exclusive/Failover：不支持 Shared 订阅
- ❌ 容错性差：某条消息失败会阻塞后续消息

**适用场景**：
- Exclusive 或 Failover 订阅模式
- 消息必须按顺序处理的场景
- 消息处理成功率高的场景
- 需要降低网络开销的场景

### 3. Negative ACK (`negative_ack.py`)

**核心代码**：
```python
consumer = client.subscribe(
    'persistent://public/default/negative-ack-topic',
    'negative-ack-subscription',
    consumer_type=pulsar.ConsumerType.Shared,
    negative_ack_redelivery_delay_ms=1000  # 1 秒后重新投递
)

while True:
    msg = consumer.receive()
    
    try:
        process_message(msg.data().decode('utf-8'))
        consumer.acknowledge(msg)
        
    except RetryableException as e:
        # 可重试的错误，发送 Nack
        print(f"⚠️ 消息处理失败，将重新投递: {e}")
        consumer.negative_acknowledge(msg)
        
    except Exception as e:
        # 不可重试的错误，确认消息（避免无限重试）
        print(f"❌ 消息无法处理，已确认: {e}")
        consumer.acknowledge(msg)
```

**特点**：
- ✅ 快速重试：立即触发消息重新投递
- ✅ 明确语义：区分"处理失败"和"不确认"
- ✅ 可配置延迟：控制重试间隔
- ❌ 需要判断：需要区分可重试和不可重试错误

**适用场景**：
- 临时性错误（网络超时、服务暂时不可用）
- 需要快速重试的场景
- 需要区分可重试和不可重试错误的场景
- 避免消息长时间阻塞的场景

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

```python
try:
    process_message(msg.data().decode('utf-8'))
    consumer.acknowledge(msg)
    
except RetryableException as e:
    # 可重试错误：发送 Nack
    consumer.negative_acknowledge(msg)
    
except PermanentException as e:
    # 不可重试错误：确认消息，避免无限重试
    consumer.acknowledge(msg)
    # 记录错误日志，人工介入
    logger.error(f"Permanent error: {e}")
```

### 3. 资源管理

使用 context manager 确保资源自动释放：
```python
client = None
consumer = None

try:
    client = pulsar.Client('pulsar://localhost:6650')
    consumer = client.subscribe(...)
    
    # 消费消息
    
finally:
    if consumer:
        consumer.close()
    if client:
        client.close()
```

### 4. Cumulative ACK 注意事项

- 只能用于 Exclusive 或 Failover 订阅模式
- 消息必须按顺序处理
- 某条消息失败会阻塞后续消息
- 适合消息处理成功率高的场景

### 5. Negative ACK 配置建议

```python
consumer = client.subscribe(
    topic='my-topic',
    subscription_name='my-subscription',
    negative_ack_redelivery_delay_ms=1000  # 根据业务调整重试延迟
)
```

## 🔧 故障排查

### 消息重复消费

**原因**：
- Consumer 崩溃前未确认消息
- ACK 超时
- 网络问题导致 ACK 丢失

**解决方案**：
```python
# 实现幂等性处理
processed_messages = set()

while True:
    msg = consumer.receive()
    message_id = str(msg.message_id())
    
    if message_id in processed_messages:
        consumer.acknowledge(msg)
        continue
    
    process_message(msg.data().decode('utf-8'))
    processed_messages.add(message_id)
    consumer.acknowledge(msg)
```

### 消息丢失

**原因**：
- 消息被错误确认
- 没有配置重试机制

**解决方案**：
```python
# 谨慎使用 acknowledge，确保消息处理成功
try:
    process_message(msg.data().decode('utf-8'))
    consumer.acknowledge(msg)  # 只在成功后确认
except Exception as e:
    # 不确认或发送 Nack
    logger.error(f"Message processing failed: {e}")
```

### Cumulative ACK 失败

**错误信息**：
```
Cumulative ack cannot be used when the subscription type is Shared
```

**解决方案**：
```python
# 使用 Exclusive 或 Failover 订阅模式
consumer = client.subscribe(
    topic='my-topic',
    subscription_name='my-subscription',
    consumer_type=pulsar.ConsumerType.Exclusive  # 或 Failover
)
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

[查看完整代码示例](https://github.com/pingxin403/apache-pulsar-examples/tree/main/python-examples/consumer-ack)
