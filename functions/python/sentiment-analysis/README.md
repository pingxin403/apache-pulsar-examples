# Python Pulsar Function - 情感分析

实时情感分析 Function，接收评论文本，输出情感标签（positive/negative/neutral）和分数。

## 📋 功能说明

- 基于关键词的情感分析（支持中英文）
- 输出 JSON 格式结果，包含情感标签、分数、匹配词
- 记录处理指标（消息数、各情感类别计数）

## 🚀 快速开始

### 部署

```bash
# 使用部署脚本
./deploy.sh

# 或手动部署
docker exec pulsar bin/pulsar-admin functions create \
    --function-config-file config.yaml
```

### 测试

```bash
# 发送正面评论
docker exec pulsar bin/pulsar-client produce reviews \
    --messages "This product is great and amazing"

# 发送负面评论
docker exec pulsar bin/pulsar-client produce reviews \
    --messages "Terrible quality, very disappointing"

# 查看分析结果
docker exec pulsar bin/pulsar-client consume sentiment-results -s test -n 2
```

### 输出示例

```json
{
  "text": "This product is great and amazing",
  "sentiment": "positive",
  "score": 1.0,
  "positive_words": ["great", "amazing"],
  "negative_words": []
}
```

## 📖 相关文章

- [Pulsar Functions 开发指南](../../../../Pulsar/03-核心功能深度篇/03-Pulsar-Functions开发指南.md)
- [基于 Pulsar Functions 的实时风控引擎](../../../../Pulsar/08-实战项目篇/04-基于PulsarFunctions的实时风控引擎.md)
