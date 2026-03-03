# Pulsar 异步操作示例 (Python)

本示例演示如何在 Python 中使用 Apache Pulsar 的异步操作功能，包括异步 Producer 和异步 Consumer。

## 📋 功能说明

异步操作提供以下优势：

- **非阻塞**: 不等待操作完成，立即返回
- **高吞吐量**: 可以同时发送多条消息
- **提高效率**: 充分利用网络和 CPU 资源
- **批量处理**: 适合批量发送和接收场景

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

#### 异步 Producer 示例
```bash
python async_producer_example.py
```

#### 异步 Consumer 示例
```bash
python async_consumer_example.py
```

## 📚 示例说明

### 1. 异步 Producer

```python
def callback(res, msg_id):
    print(f"消息发送成功: {msg_id}")

# 异步发送消息
producer.send_async(
    message.encode('utf-8'),
    callback=callback
)

# 等待所有异步发送完成
producer.flush()
```


**特点**：
- 不阻塞主线程
- 通过回调函数处理结果
- 使用 `flush()` 等待所有异步操作完成

### 2. 异步 Consumer

```python
# 接收消息（非阻塞）
msg = consumer.receive(timeout_millis=5000)

# 异步确认
consumer.acknowledge(msg)
```

**特点**：
- 设置超时时间避免无限等待
- 快速处理消息
- 提高消费效率

## 🔍 同步 vs 异步对比

| 特性 | 同步操作 | 异步操作 |
|------|----------|----------|
| 阻塞 | 阻塞等待 | 非阻塞 |
| 吞吐量 | 低 | 高 |
| 延迟 | 高 | 低 |
| 复杂度 | 简单 | 稍复杂 |
| 适用场景 | 简单场景 | 高性能场景 |

## 💡 使用场景

### 异步 Producer
- 批量数据导入
- 日志收集系统
- 实时数据采集

### 异步 Consumer
- 高吞吐量消费
- 实时数据处理
- 流式计算

## ⚠️ 注意事项

1. **回调函数**: 异步发送需要提供回调函数处理结果
2. **错误处理**: 需要在回调中处理发送失败的情况
3. **资源管理**: 使用 `flush()` 确保所有消息发送完成
4. **超时设置**: Consumer 需要设置合理的超时时间

## 📖 相关文档

- [Pulsar Python Client 官方文档](https://pulsar.apache.org/docs/client-libraries-python/)
- [技术文章：Producer发送模式](../../Pulsar/01-入门篇/06-Producer发送模式.md)

## 🔗 完整代码

[查看完整代码示例](https://github.com/pingxin403/apache-pulsar-examples/tree/main/python-examples/async-operations)
