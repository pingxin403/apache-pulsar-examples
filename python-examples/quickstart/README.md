# Pulsar Python 快速入门示例

本示例演示如何使用 Python 快速上手 Apache Pulsar，包括基本的 Producer 和 Consumer 操作。

## 📋 功能说明

本示例展示 Pulsar 的核心功能：

- **Producer**: 创建生产者并发送消息到 Pulsar Topic
- **Consumer**: 创建消费者并从 Pulsar Topic 接收消息
- **资源管理**: 正确关闭客户端连接和释放资源
- **消息确认**: 使用 ACK 机制确认消息处理完成

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

或直接安装：

```bash
pip install pulsar-client==3.2.0
```

### 2. 启动 Pulsar

使用 Docker Compose 启动 Pulsar 环境：

```bash
cd ../../docker-compose
docker-compose up -d
```

或使用 Docker 命令启动单机模式：

```bash
docker run -it \
  -p 6650:6650 \
  -p 8080:8080 \
  --name pulsar-standalone \
  apachepulsar/pulsar:3.2.0 \
  bin/pulsar standalone
```

### 3. 运行示例

#### 运行 Producer（发送消息）

```bash
python producer.py
```

输出：
```
✅ 消息已发送
```

#### 运行 Consumer（接收消息）

在另一个终端窗口运行：

```bash
python consumer.py
```

输出：
```
📩 收到消息: Hello from Python!
```

## 📚 示例说明

### Producer 示例

```python
import pulsar

# 1. 创建 Pulsar 客户端
client = pulsar.Client('pulsar://localhost:6650')

# 2. 创建生产者
producer = client.create_producer('my-python-topic')

try:
    # 3. 发送消息
    producer.send(b"Hello from Python!")
    print("✅ 消息已发送")
finally:
    # 4. 关闭资源
    producer.close()
    client.close()
```

**关键代码解释**：

1. **创建客户端**: `pulsar.Client()` 连接到 Pulsar 服务，默认地址为 `pulsar://localhost:6650`
2. **创建生产者**: `create_producer()` 指定 Topic 名称，Pulsar 会自动创建不存在的 Topic
3. **发送消息**: `send()` 方法发送字节数组消息，返回消息 ID
4. **资源释放**: 使用 `try-finally` 确保资源正确关闭

### Consumer 示例

```python
import pulsar

# 1. 创建 Pulsar 客户端
client = pulsar.Client('pulsar://localhost:6650')

# 2. 创建消费者
consumer = client.subscribe('my-python-topic', 'python-sub')

try:
    # 3. 接收消息
    msg = consumer.receive()
    print("📩 收到消息:", msg.data().decode('utf-8'))
    
    # 4. 确认消息
    consumer.acknowledge(msg)
finally:
    # 5. 关闭资源
    consumer.close()
    client.close()
```

**关键代码解释**：

1. **创建客户端**: 与 Producer 相同，连接到 Pulsar 服务
2. **订阅 Topic**: `subscribe()` 需要指定 Topic 名称和订阅名称（Subscription Name）
3. **接收消息**: `receive()` 阻塞等待消息，返回 Message 对象
4. **消息确认**: `acknowledge()` 告诉 Pulsar 消息已成功处理，避免重复消费
5. **资源释放**: 确保连接正确关闭

## 💡 核心概念

### Topic（主题）

Topic 是 Pulsar 中消息的逻辑通道，类似于消息队列的队列名称。

- **命名规则**: `persistent://tenant/namespace/topic-name`
- **简化写法**: 直接使用 `my-python-topic`，Pulsar 会自动补全为 `persistent://public/default/my-python-topic`

### Subscription（订阅）

订阅是消费者组的概念，多个消费者可以共享同一个订阅。

- **订阅名称**: 用于标识消费者组，如 `python-sub`
- **消息位置**: Pulsar 为每个订阅维护消费位置（Cursor）
- **持久化**: 订阅信息持久化存储，重启后可继续消费

### Message Acknowledgment（消息确认）

消息确认机制确保消息不会丢失：

- **ACK**: 确认消息已成功处理
- **NACK**: 告诉 Pulsar 消息处理失败，需要重新投递
- **未确认**: 如果消费者崩溃，未确认的消息会重新投递

## 🔍 使用场景

### 适用场景

1. **异步通信**: 微服务之间的异步消息传递
2. **事件驱动**: 发布-订阅模式的事件通知
3. **任务队列**: 分布式任务处理
4. **日志收集**: 应用日志的集中收集和处理
5. **数据管道**: 实时数据流处理

### 示例场景

**场景 1: 订单处理系统**
- Producer: 订单服务发送新订单消息
- Consumer: 库存服务、支付服务、物流服务分别订阅处理

**场景 2: 实时日志分析**
- Producer: 应用服务器发送日志消息
- Consumer: 日志分析服务实时处理和存储

## ⚠️ 注意事项

1. **Python 版本**: 需要 Python 3.7 或更高版本
2. **依赖安装**: 确保安装 `pulsar-client==3.2.0`
3. **Pulsar 服务**: 运行示例前确保 Pulsar 服务已启动
4. **资源释放**: 始终在 `finally` 块中关闭客户端和生产者/消费者
5. **消息格式**: `send()` 方法接受字节数组，字符串需要编码为 `bytes`
6. **阻塞接收**: `receive()` 会阻塞等待消息，生产环境建议使用超时参数

## 🚀 进阶使用

### 设置超时

```python
# 接收消息时设置超时（毫秒）
msg = consumer.receive(timeout_millis=5000)
```

### 异步发送

```python
# 异步发送消息
producer.send_async(b"Hello", callback=lambda res, msg_id: print(f"发送成功: {msg_id}"))
```

### 批量发送

```python
# 启用批量发送
producer = client.create_producer(
    'my-topic',
    batching_enabled=True,
    batching_max_messages=100
)
```

## 📖 相关文档

- [Pulsar Python Client 官方文档](https://pulsar.apache.org/docs/client-libraries-python/)
- [技术文章：5分钟上手Pulsar](../../../Pulsar/01-入门篇/01-5分钟上手Pulsar.md)
- [技术文章：统一消息模型](../../../Pulsar/01-入门篇/04-统一消息模型.md)

## 🔗 完整代码

[查看完整代码示例](https://github.com/pingxin403/apache-pulsar-examples/tree/main/python-examples/quickstart)
