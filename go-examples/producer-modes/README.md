# Producer 发送模式示例 (Go)

对应文章: `Pulsar/01-入门篇/06-Producer发送模式.md`

## 前置条件

1. 安装 Go 1.19+
2. 安装依赖:
```bash
go mod download
```
3. 启动 Pulsar 单机容器:
```bash
cd ../../docker-compose
docker-compose up -d
```

## 示例列表

### 1. 同步发送 (sync_producer.go)

演示如何使用同步发送模式，确保消息可靠送达。

**运行**:
```bash
go run sync_producer.go
```

**适用场景**:
- 支付订单确认
- 重要通知
- 需要立即确认的业务

**特点**:
- ✅ 可靠性高，能立即知道发送结果
- ❌ 吞吐量较低，每次发送都需要等待响应

**关键代码**:
```go
// 同步发送消息
msgID, err := producer.Send(ctx, &pulsar.ProducerMessage{
    Payload: []byte("订单 12345 支付成功"),
})
if err != nil {
    log.Fatalf("发送失败: %v", err)
}
fmt.Printf("消息发送成功，MessageId: %v\n", msgID)
```

### 2. 异步发送 (async_producer.go)

演示如何使用异步发送模式，提高吞吐量。

**运行**:
```bash
go run async_producer.go
```

**适用场景**:
- 用户行为日志
- 实时监控数据
- 高并发场景

**特点**:
- ✅ 吞吐量高，不阻塞主线程
- ❌ 需要处理回调，错误处理相对复杂

**关键代码**:
```go
// 异步发送消息
producer.SendAsync(ctx, &pulsar.ProducerMessage{
    Payload: []byte(message),
}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
    if err != nil {
        log.Printf("发送失败: %v", err)
    } else {
        log.Printf("发送成功: %v", id)
    }
})

// 等待所有异步消息发送完成
wg.Wait()
```

### 3. 批量发送 (batch_producer.go)

演示如何使用批量发送模式，降低网络开销。

**运行**:
```bash
go run batch_producer.go
```

**适用场景**:
- IoT 设备数据上报
- 日志收集
- 高吞吐量场景

**特点**:
- ✅ 降低网络开销，提高吞吐量
- ❌ 增加了消息延迟（需要等待批次填满或超时）

**配置参数**:
```go
producer, err := client.CreateProducer(pulsar.ProducerOptions{
    Topic:                   "persistent://public/default/iot-sensor-data",
    BatchingMaxMessages:     100,                      // 每批最多 100 条消息
    BatchingMaxPublishDelay: 10 * time.Millisecond,   // 最多等待 10ms
})
```

**关键代码**:
```go
// 发送消息（自动批量）
producer.SendAsync(ctx, &pulsar.ProducerMessage{
    Payload: []byte(message),
}, callback)

// 刷新所有批次
err = producer.Flush()
```

## 性能对比

| 发送模式 | 吞吐量 | 延迟 | 可靠性 | 适用场景 |
|---------|--------|------|--------|---------|
| 同步发送 | 低 | 高 | 高 | 支付、重要通知 |
| 异步发送 | 高 | 中 | 高 | 日志、监控数据 |
| 批量发送 | 很高 | 低 | 高 | IoT、日志收集 |

## Go 特性说明

### Context 管理
所有示例都使用 `context.Context` 来管理生命周期和超时：

```go
ctx := context.Background()
// 或者使用带超时的 context
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
```

### 错误处理
遵循 Go 的错误处理惯例：

```go
if err != nil {
    log.Fatalf("操作失败: %v", err)
    return
}
```

### 资源释放
使用 `defer` 确保资源正确释放：

```go
defer client.Close()
defer producer.Close()
```

### 并发控制
异步发送示例使用 `sync.WaitGroup` 和 `sync.Mutex` 进行并发控制：

```go
var wg sync.WaitGroup
var mu sync.Mutex

wg.Add(1)
producer.SendAsync(ctx, msg, func(...) {
    defer wg.Done()
    mu.Lock()
    defer mu.Unlock()
    // 处理结果
})

wg.Wait()
```

## 相关文章

- [Producer 发送模式全解析：同步/异步/批量/带 Key 路由](../../../Pulsar/01-入门篇/06-Producer发送模式.md)

## 相关示例

- [Go 快速入门示例](../quickstart/)
- [Java Producer 模式示例](../../java-examples/producer-modes/)
- [Python Producer 模式示例](../../python-examples/producer-modes/)
