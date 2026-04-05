# 实时用户行为分析 Pipeline

基于 Pulsar 的实时用户行为分析系统，包含事件生产、实时聚合和结果输出。

## 📋 架构

```
用户行为模拟器 → [user-events Topic] → 事件处理器 → [analytics-results Topic]
(event-producer)                       (event-processor)
```

## 🚀 快速开始

### 方式一：Docker Compose（推荐）

```bash
docker-compose up -d
```

### 方式二：手动运行

```bash
# 1. 启动 Pulsar
cd ../../docker-compose && docker-compose up -d

# 2. 启动事件生产者
cd event-producer
pip install -r requirements.txt
python producer.py

# 3. 启动事件处理器（新终端）
cd event-processor
pip install -r requirements.txt
python processor.py

# 4. 查看分析结果（新终端）
docker exec pulsar bin/pulsar-client consume analytics-results -s viewer -n 0
```

## 📊 事件格式

```json
{
  "user_id": "user_0042",
  "event_type": "purchase",
  "page": "/checkout",
  "device": "mobile",
  "region": "us-east",
  "timestamp": "2024-01-01T12:00:00Z",
  "amount": 99.99,
  "product_id": "prod_123"
}
```

## 📖 相关文章

- [用 Pulsar 构建实时用户行为分析 pipeline](../../../Pulsar/08-实战项目篇/01-用Pulsar构建实时用户行为分析pipeline.md)
