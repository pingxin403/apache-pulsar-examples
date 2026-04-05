# Go Schema 使用示例

本示例演示 Apache Pulsar Go 客户端的 Schema 功能，包括 JSON Schema 和 Avro Schema。

## 📋 功能说明

| 示例 | 文件 | 说明 |
|------|------|------|
| **JSON Schema** | `json_schema_example.go` | 使用 JSON Schema 进行类型安全的消息传输 |
| **Avro Schema** | `avro_schema_example.go` | 使用 Avro Schema 支持 Schema 演进 |

## 🚀 快速开始

### 前置条件

- Go 1.21+
- Pulsar running locally (see `../../docker-compose/`)

### 运行示例

```bash
# JSON Schema 示例
go run json_schema_example.go

# Avro Schema 示例
go run avro_schema_example.go
```

## 📖 相关文章

- [Schema Registry 深度集成](../../../Pulsar/03-核心功能深度篇/05-Schema-Registry深度集成.md)
