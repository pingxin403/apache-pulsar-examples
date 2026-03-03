# Pulsar Go 快速入门示例

本示例演示如何使用 Go 语言快速上手 Apache Pulsar，包括基本的 Producer 和 Consumer 操作。

## 📋 功能说明

- Producer: 发送消息到 Pulsar Topic
- Consumer: 从 Pulsar Topic 接收消息

## 🚀 快速开始

### 1. 安装依赖

```bash
go mod download
```

### 2. 启动 Pulsar

```bash
cd ../../docker-compose
docker-compose up -d
```

### 3. 运行示例

#### 运行 Producer（发送消息）
```bash
go run producer.go
```

#### 运行 Consumer（接收消息）
```bash
go run consumer.go
```

## 📚 示例说明

### Producer 示例

```go
// 创建客户端
client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})

// 创建 Producer
producer, err := client.CreateProducer(pulsar.ProducerOptions{
    Topic: "persistent://public/default/go-quickstart-topic",
})

// 发送消息
msgID, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
    Payload: []byte("Hello Pulsar"),
})
```

### Consumer 示例

```go
// 创建 Consumer
consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    Topic:            "persistent://public/default/go-quickstart-topic",
    SubscriptionName: "go-quickstart-subscription",
    Type:             pulsar.Exclusive,
})

// 接收消息
msg, err := consumer.Receive(context.Background())
fmt.Printf("接收: %s\n", string(msg.Payload()))

// 确认消息
consumer.Ack(msg)
```

## 📖 相关文档

- [Pulsar Go Client 官方文档](https://pulsar.apache.org/docs/client-libraries-go/)
- [技术文章：5分钟上手Pulsar](../../Pulsar/01-入门篇/01-5分钟上手Pulsar.md)

## 🔗 完整代码

[查看完整代码示例](https://github.com/pingxin403/apache-pulsar-examples/tree/main/go-examples/quickstart)
