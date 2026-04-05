# File Source Connector

从文件系统读取数据并发送到 Pulsar Topic 的 Source Connector 示例。

## 📋 功能说明

- 监控指定目录中的新文件
- 读取文件内容并发送到 Pulsar Topic
- 处理完成后自动移动文件到已处理目录
- 支持文件过滤和轮询间隔配置

## 🚀 快速开始

### 部署

```bash
./deploy.sh
```

### 测试

```bash
# 创建测试文件
docker exec pulsar bash -c \
    'echo "Hello from file source" > /tmp/pulsar-file-source/input/test.txt'

# 查看输出
docker exec pulsar bin/pulsar-client consume file-source-output -s test -n 1

# 查看状态
docker exec pulsar bin/pulsar-admin sources status --name file-source
```

### 清理

```bash
docker exec pulsar bin/pulsar-admin sources delete --name file-source
```

## 📖 相关文章

- [IO Connectors 实战](../../../../Pulsar/03-核心功能深度篇/04-IO-Connectors实战.md)
