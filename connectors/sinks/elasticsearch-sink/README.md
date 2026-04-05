# Elasticsearch Sink Connector

从 Pulsar Topic 读取消息并写入 Elasticsearch 的 Sink Connector 示例。

## 📋 功能说明

- 从 Pulsar Topic 消费消息
- 批量写入 Elasticsearch
- 支持自动创建索引
- 包含 Kibana 可视化

## 🚀 快速开始

### 1. 启动 Elasticsearch + Kibana

```bash
docker-compose up -d
```

### 2. 部署 Sink Connector

```bash
./deploy.sh
```

### 3. 测试

```bash
# 发送 JSON 消息
docker exec pulsar bin/pulsar-client produce es-sink-input \
    --messages '{"name":"test-event","value":42,"timestamp":"2024-01-01T00:00:00Z"}'

# 查看 Elasticsearch 数据
curl http://localhost:9200/pulsar-messages/_search?pretty

# 查看 Connector 状态
docker exec pulsar bin/pulsar-admin sinks status --name elasticsearch-sink
```

### 4. Kibana 可视化

访问 http://localhost:5601 创建 Index Pattern `pulsar-messages*` 查看数据。

### 清理

```bash
docker exec pulsar bin/pulsar-admin sinks delete --name elasticsearch-sink
docker-compose down -v
```

## 📖 相关文章

- [IO Connectors 实战](../../../../Pulsar/03-核心功能深度篇/04-IO-Connectors实战.md)
- [日志中心化收集](../../../../Pulsar/08-实战项目篇/05-日志中心化收集.md)
