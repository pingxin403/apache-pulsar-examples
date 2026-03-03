# Pulsar Schema 使用示例 (Python)

本示例演示如何在 Python 中使用 Apache Pulsar 的 Schema 功能，包括 Avro Schema 和 JSON Schema。

## 📋 功能说明

Schema 提供以下优势：

- **类型安全**: 在运行时检查消息类型
- **数据验证**: 自动验证消息格式
- **版本管理**: 支持 Schema 版本演进
- **跨语言兼容**: 不同语言客户端可以共享 Schema

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

#### Avro Schema 示例
```bash
python avro_schema_example.py
```

#### JSON Schema 示例
```bash
python json_schema_example.py
```

## 📚 示例说明

### 1. 定义 Schema 类

```python
from pulsar.schema import Record, String, Integer

class User(Record):
    name = String()
    age = Integer()
    email = String()
```

### 2. Avro Schema 示例

```python
from pulsar.schema import AvroSchema

# 创建 Producer
producer = client.create_producer(
    TOPIC,
    schema=AvroSchema(User)
)

# 发送消息
user = User(name='Alice', age=30, email='alice@example.com')
producer.send(user)

# 创建 Consumer
consumer = client.subscribe(
    TOPIC,
    subscription_name='avro-subscription',
    schema=AvroSchema(User)
)

# 接收消息
msg = consumer.receive()
user = msg.value()
print(f"{user.name}, {user.age}, {user.email}")
```

### 3. JSON Schema 示例

```python
from pulsar.schema import JsonSchema

# 创建 Producer
producer = client.create_producer(
    TOPIC,
    schema=JsonSchema(User)
)

# 发送消息
user = User(name='Bob', age=25, email='bob@example.com')
producer.send(user)

# 创建 Consumer
consumer = client.subscribe(
    TOPIC,
    subscription_name='json-subscription',
    schema=JsonSchema(User)
)

# 接收消息
msg = consumer.receive()
user = msg.value()
print(f"{user.name}, {user.age}, {user.email}")
```

## 🔍 Schema 对比

| 特性 | Avro Schema | JSON Schema |
|------|-------------|-------------|
| 格式 | 二进制 | 文本 |
| 性能 | 高 | 中 |
| 可读性 | 低 | 高 |
| 压缩率 | 高 | 低 |
| 适用场景 | 生产环境 | 开发调试 |

## 💡 支持的数据类型

### 基本类型
- `String()` - 字符串
- `Integer()` - 整数
- `Long()` - 长整数
- `Float()` - 浮点数
- `Double()` - 双精度浮点数
- `Boolean()` - 布尔值
- `Bytes()` - 字节数组

### 复杂类型
- `Array(item_type)` - 数组
- `Map(value_type)` - 字典

### 示例：复杂类型

```python
from pulsar.schema import Record, String, Integer, Array, Map

class Address(Record):
    street = String()
    city = String()
    zipcode = String()

class Employee(Record):
    name = String()
    age = Integer()
    addresses = Array(Address())
    metadata = Map(String())
```

## ⚠️ 注意事项

1. **依赖安装**: Avro Schema 需要安装 `fastavro` 库
2. **类型匹配**: Producer 和 Consumer 必须使用相同的 Schema
3. **版本兼容**: Schema 变更需要考虑向后兼容性
4. **性能影响**: Schema 验证会增加少量开销

## 📖 相关文档

- [Pulsar Schema 官方文档](https://pulsar.apache.org/docs/schema-get-started/)
- [Python Client Schema 文档](https://pulsar.apache.org/docs/client-libraries-python/#schema)
- [技术文章：Schema Registry深度集成](../../Pulsar/03-核心功能深度篇/05-Schema-Registry深度集成.md)

## 🔗 完整代码

[查看完整代码示例](https://github.com/pingxin403/apache-pulsar-examples/tree/main/python-examples/schema-usage)
