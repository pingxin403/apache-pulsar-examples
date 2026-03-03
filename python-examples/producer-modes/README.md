# Producer 发送模式示例 (Python)

对应文章: `Pulsar/01-入门篇/06-Producer发送模式.md`

## 前置条件

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

## 示例列表

### 1. 同步发送 (sync_producer.py)

演示如何使用同步发送模式，确保消息可靠送达。

**运行**:
```bash
python sync_producer.py
```

**适用场景**:
- 支付订单确认
- 重要通知
- 需要立即确认的业务

**特点**:
- ✅ 可靠性高，能立即知道发送结果
- ❌ 吞吐量较低，每次发送都需要等待响应

### 2. 异步发送 (async_producer.py)

演示如何使用异步发送模式，提高吞吐量。

**运行**:
```bash
python async_producer.py
```

**适用场景**:
- 用户行为日志
- 实时监控数据
- 高并发场景

**特点**:
- ✅ 吞吐量高，不阻塞主线程
- ❌ 需要处理回调，错误处理相对复杂

### 3. 批量发送 (batch_producer.py)

演示如何使用批量发送模式，降低网络开销。

**运行**:
```bash
python batch_producer.py
```

**适用场景**:
- IoT 设备数据上报
- 日志收集
- 高吞吐量场景

**特点**:
- ✅ 降低网络开销，提高吞吐量
- ❌ 增加了消息延迟（需要等待批次填满或超时）

**配置参数**:
- `batching_enabled=True`: 启用批量发送
- `batching_max_messages=100`: 每批最多 100 条消息
- `batching_max_publish_delay_ms=10`: 最多等待 10ms

### 4. 带 Key 路由 (key_based_producer.py)

演示如何使用 Key 路由，保证消息顺序。

**运行**:
```bash
python key_based_producer.py
```

**适用场景**:
- 用户行为分析（保证同一用户的事件顺序）
- 订单状态机（保证同一订单的状态变更顺序）
- IoT 设备数据（保证同一设备的数据顺序）

**特点**:
- ✅ 保证相同 Key 的消息顺序
- ✅ 支持 Key_Shared 订阅模式
- ❌ 可能导致分区负载不均衡

## 性能对比

| 发送模式 | 吞吐量 | 延迟 | 可靠性 | 适用场景 |
|---------|--------|------|--------|---------|
| 同步发送 | 低 | 高 | 高 | 支付、重要通知 |
| 异步发送 | 高 | 中 | 高 | 日志、监控数据 |
| 批量发送 | 很高 | 低 | 高 | IoT、日志收集 |
| 带 Key 路由 | 中 | 中 | 高 | 顺序保证场景 |

## 相关文章

- [Producer 发送模式全解析：同步/异步/批量/带 Key 路由](../../../Pulsar/01-入门篇/06-Producer发送模式.md)
