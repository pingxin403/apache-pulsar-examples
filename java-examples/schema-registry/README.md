# Pulsar Schema Registry 示例

本示例演示如何在 Apache Pulsar 中使用 Schema Registry 功能，包括 Avro Schema 和 JSON Schema。

## 📋 功能说明

Schema Registry 是 Pulsar 的核心特性之一，提供以下优势：

- **类型安全**: 在编译时检查消息类型
- **数据验证**: 自动验证消息格式
- **版本管理**: 支持 Schema 版本演进
- **跨语言兼容**: 不同语言客户端可以共享 Schema

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

#### Avro Schema 示例
```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.AvroSchemaExample"
```

#### JSON Schema 示例
```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.JsonSchemaExample"
```

## 📚 示例说明

### 1. Avro Schema 示例 (`AvroSchemaExample.java`)

使用 Avro 格式序列化和反序列化消息：

```java
// 创建 Producer
Producer<User> producer = client.newProducer(AvroSchema.of(User.class))
        .topic(TOPIC)
        .create();

// 发送消息
User user = new User("Alice", 30, "alice@example.com");
producer.send(user);

// 创建 Consumer
Consumer<User> consumer = client.newConsumer(AvroSchema.of(User.class))
        .topic(TOPIC)
        .subscriptionName("avro-subscription")
        .subscribe();
```

**特点**：
- 二进制格式，高效压缩
- 强类型检查
- 支持复杂数据结构
- 适合高性能场景

### 2. JSON Schema 示例 (`JsonSchemaExample.java`)

使用 JSON 格式序列化和反序列化消息：

```java
// 创建 Producer
Producer<User> producer = client.newProducer(JSONSchema.of(User.class))
        .topic(TOPIC)
        .create();

// 发送消息
User user = new User("Bob", 25, "bob@example.com");
producer.send(user);

// 创建 Consumer
Consumer<User> consumer = client.newConsumer(JSONSchema.of(User.class))
        .topic(TOPIC)
        .subscriptionName("json-subscription")
        .subscribe();
```

**特点**：
- 文本格式，易于调试
- 人类可读
- 跨语言兼容性好
- 适合开发和调试

## 🔍 Schema 对比

| 特性 | Avro Schema | JSON Schema |
|------|-------------|-------------|
| 格式 | 二进制 | 文本 |
| 性能 | 高 | 中 |
| 可读性 | 低 | 高 |
| 压缩率 | 高 | 低 |
| 适用场景 | 生产环境 | 开发调试 |

## 📖 相关文档

- [Pulsar Schema Registry 官方文档](https://pulsar.apache.org/docs/schema-get-started/)
- [技术文章：Schema Registry深度集成](../../Pulsar/03-核心功能深度篇/05-Schema-Registry深度集成.md)

## 🔗 完整代码

[查看完整代码示例](https://github.com/pingxin403/apache-pulsar-examples/tree/main/java-examples/schema-registry)
