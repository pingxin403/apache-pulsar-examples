# Java Pulsar Function - Word Count

实时单词计数 Function，接收文本消息，统计每个单词的累计出现次数。

## 📋 功能说明

- 接收文本消息，按空格分词
- 使用 Pulsar Function State 存储累计计数
- 输出 `word:count` 格式的结果
- 支持中英文单词

## 🚀 快速开始

### 1. 编译

```bash
mvn clean package
```

### 2. 部署

```bash
# 使用部署脚本
./deploy.sh

# 或手动部署
docker exec pulsar bin/pulsar-admin functions create \
    --function-config-file config.yaml \
    --jar target/word-count-function-1.0.0.jar
```

### 3. 测试

```bash
# 发送消息
docker exec pulsar bin/pulsar-client produce sentences \
    --messages "hello world hello pulsar"

# 查看输出
docker exec pulsar bin/pulsar-client consume word-counts -s test -n 1

# 查看状态
docker exec pulsar bin/pulsar-admin functions status --name word-count
```

## 📖 相关文章

- [Pulsar Functions 开发指南](../../../../Pulsar/03-核心功能深度篇/03-Pulsar-Functions开发指南.md)
