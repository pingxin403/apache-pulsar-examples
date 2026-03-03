# Apache Pulsar Code Examples

![CI](https://github.com/pingxin403/apache-pulsar-examples/workflows/CI/badge.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Pulsar Version](https://img.shields.io/badge/Pulsar-3.2.0-brightgreen.svg)](https://pulsar.apache.org/)

[English](#english) | [中文](#中文)

---

## English

### 📖 About

This repository contains complete, runnable code examples for the Apache Pulsar technical documentation series (77 articles). It covers Java, Python, and Go client examples, along with Docker Compose configurations, Pulsar Functions, and IO Connectors.

### 🎯 Project Goals

- Provide runnable code examples for each technical article
- Help developers quickly get started with Pulsar features
- Offer one-click local development environment deployment
- Demonstrate Pulsar best practices across different programming languages

### 📁 Project Structure

```
apache-pulsar-examples/
├── java-examples/          # Java client examples (20+ examples)
│   ├── quickstart/         # Quick start: Producer/Consumer basics
│   ├── producer-modes/     # Producer modes: sync/async/batch/key-based
│   ├── consumer-ack/       # Consumer ACK mechanisms
│   ├── key-shared-demo/    # Key_Shared subscription
│   ├── subscription-modes/ # Subscription modes comparison
│   ├── schema-registry/    # Schema usage (Avro/Protobuf/JSON)
│   ├── transactions/       # Transaction messages
│   ├── delayed-messages/   # Delayed messages
│   ├── dead-letter-topic/  # Dead Letter Queue (DLQ)
│   └── ...                 # More examples
├── python-examples/        # Python client examples (18+ examples)
│   ├── quickstart/         # Quick start
│   ├── producer-modes/     # Producer modes
│   ├── consumer-ack/       # Consumer ACK mechanisms
│   ├── key-shared-demo/    # Key_Shared subscription
│   ├── subscription-modes/ # Subscription modes
│   ├── schema-usage/       # Schema usage
│   └── ...                 # More examples
├── go-examples/            # Go client examples (15+ examples)
│   ├── quickstart/         # Quick start
│   ├── producer-modes/     # Producer modes
│   ├── consumer-ack/       # Consumer ACK mechanisms
│   └── ...                 # More examples
├── docker-compose/         # Docker Compose configurations
│   ├── docker-compose-standalone.yml  # Standalone mode
│   ├── docker-compose-cluster.yml     # Cluster mode
│   └── docker-compose-monitoring.yml  # Monitoring mode
├── functions/              # Pulsar Functions examples
│   ├── java/               # Java Functions
│   ├── python/             # Python Functions
│   └── go/                 # Go Functions
├── connectors/             # IO Connectors examples
│   ├── sources/            # Source Connectors
│   └── sinks/              # Sink Connectors
├── advanced-examples/      # Advanced scenarios
├── projects/               # Real-world project examples
├── benchmarks/             # Performance testing examples
└── tests/                  # Test framework
```

### 🚀 Quick Start

#### Prerequisites

- Docker 20.10+
- Docker Compose 2.0+
- JDK 11+ (for Java examples)
- Python 3.7+ (for Python examples)
- Go 1.19+ (for Go examples)

#### Step 1: Start Pulsar Environment

**Option 1: Standalone Mode (Recommended for development)**

```bash
cd docker-compose
docker-compose -f docker-compose-standalone.yml up -d
```

**Option 2: Cluster Mode (For production simulation)**

```bash
cd docker-compose
docker-compose -f docker-compose-cluster.yml up -d
```

**Option 3: With Monitoring (Prometheus + Grafana)**

```bash
cd docker-compose
docker-compose -f docker-compose-monitoring.yml up -d
```

Wait for services to be ready (~2 minutes). Verify with:

```bash
docker exec pulsar bin/pulsar-admin brokers healthcheck
```

#### Step 2: Run Your First Example

**Java Example:**

```bash
cd java-examples/quickstart
mvn clean package
mvn exec:java -Dexec.mainClass="com.example.pulsar.ProducerExample"
# In another terminal
mvn exec:java -Dexec.mainClass="com.example.pulsar.ConsumerExample"
```

**Python Example:**

```bash
cd python-examples/quickstart
pip install -r requirements.txt
python producer.py
# In another terminal
python consumer.py
```

**Go Example:**

```bash
cd go-examples/quickstart
go mod download
go run producer.go
# In another terminal
go run consumer.go
```

### 📚 Examples by Category

#### Basic Examples

| Category | Java | Python | Go | Description |
|----------|------|--------|----|-----------

### 📚 Examples by Category

#### Basic Examples

| Category | Java | Python | Go | Description |
|----------|------|--------|----|-----------  |
| Quick Start | ✅ | ✅ | ✅ | Basic Producer/Consumer |
| Producer Modes | ✅ | ✅ | ✅ | Sync/Async/Batch/Key-based |
| Consumer ACK | ✅ | ✅ | ✅ | Individual/Cumulative/Negative ACK |
| Subscription Modes | ✅ | ✅ | ✅ | Exclusive/Shared/Failover/Key_Shared |

#### Advanced Examples

| Category | Java | Python | Go | Description |
|----------|------|--------|----|-----------  |
| Schema Registry | ✅ | ✅ | ✅ | Avro/Protobuf/JSON Schema |
| Transactions | ✅ | ⏳ | ⏳ | Exactly-Once semantics |
| Delayed Messages | ✅ | ✅ | ✅ | Order timeout scenarios |
| Dead Letter Topic | ✅ | ✅ | ✅ | Failed message handling |
| Message Deduplication | ✅ | ✅ | ⏳ | Duplicate message prevention |

#### Pulsar Functions

| Category | Java | Python | Go | Description |
|----------|------|--------|----|-----------  |
| Word Count | ✅ | ⏳ | ⏳ | Classic MapReduce example |
| Data Enrichment | ✅ | ⏳ | ⏳ | Query external data sources |
| Sentiment Analysis | ⏳ | ✅ | ⏳ | NLP processing |
| Log Parser | ⏳ | ⏳ | ✅ | Log parsing |

#### IO Connectors

| Type | Connector | Status | Description |
|------|-----------|--------|-------------|
| Source | File Source | ✅ | Read local files |
| Source | JDBC Source | ✅ | MySQL CDC |
| Source | Kafka Source | ✅ | Kafka migration |
| Sink | Elasticsearch Sink | ✅ | Log storage |
| Sink | JDBC Sink | ✅ | Write to PostgreSQL |
| Sink | Kafka Sink | ✅ | Bidirectional sync |

#### Real-World Projects

| Project | Status | Description |
|---------|--------|-------------|
| Realtime Analytics | ⏳ | User behavior analysis pipeline |
| Payment System | ⏳ | Financial payment with transactions |
| IoT Platform | ⏳ | Massive device message ingestion |
| Log Aggregation | ⏳ | Centralized log collection |
| Risk Control Engine | ⏳ | Real-time risk control with Functions |

Legend: ✅ Completed | ⏳ In Progress | ❌ Planned

### 📖 Article Mapping

Complete mapping between technical articles and code examples:

#### 入门篇 (Getting Started)

| Article | Java Example | Python Example | Status |
|---------|--------------|----------------|--------|
| 01-5分钟上手Pulsar | [quickstart](java-examples/quickstart/) | [quickstart](python-examples/quickstart/) | ✅ |
| 06-Producer发送模式 | [producer-modes](java-examples/producer-modes/) | [producer-modes](python-examples/producer-modes/) | ✅ |
| 07-Consumer-ACK机制 | [consumer-ack](java-examples/consumer-ack/) | [consumer-ack](python-examples/consumer-ack/) | ✅ |

#### 核心功能深度篇 (Core Features)

| Article | Java Example | Python Example | Status |
|---------|--------------|----------------|--------|
| 01-Key_Shared订阅实战 | [key-shared-demo](java-examples/key-shared-demo/) | [key-shared-demo](python-examples/key-shared-demo/) | ✅ |

Legend: ✅ Completed | ⏳ In Progress | 📝 Planned

See [ARTICLE_MAPPING.md](ARTICLE_MAPPING.md) for the complete mapping between articles and code examples.

### 🔧 Development Environment

#### Java

- **JDK**: 11+
- **Build Tool**: Maven 3.6+
- **Pulsar Client**: 3.2.0
- **IDE**: IntelliJ IDEA / Eclipse

#### Python

- **Python**: 3.7+
- **Package Manager**: pip
- **Pulsar Client**: pulsar-client 3.2.0
- **IDE**: PyCharm / VS Code

#### Go

- **Go**: 1.19+
- **Package Manager**: go mod
- **Pulsar Client**: github.com/apache/pulsar-client-go
- **IDE**: GoLand / VS Code

### 🧪 Testing

Run all tests:

```bash
# Java examples
./test-build.sh java

# Python examples
./test-build.sh python

# Go examples
./test-build.sh go

# Integration tests
./test-integration.sh
```

### 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

### 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

### 🔗 Resources

- [Apache Pulsar Official Documentation](https://pulsar.apache.org/docs/)
- [Pulsar Technical Articles (Chinese)](../Pulsar/)
- [GitHub Repository](https://github.com/pingxin403/apache-pulsar-examples)
- [Issue Tracker](https://github.com/pingxin403/apache-pulsar-examples/issues)

### 📞 Support

- GitHub Issues: [Report a bug](https://github.com/pingxin403/apache-pulsar-examples/issues/new)
- Discussions: [Ask a question](https://github.com/pingxin403/apache-pulsar-examples/discussions)

---

## 中文

### 📖 关于本项目

本仓库包含 Apache Pulsar 技术文档系列（77篇文章）的完整、可运行的代码示例，涵盖 Java、Python、Go 三种编程语言的客户端示例，以及 Docker Compose 配置、Pulsar Functions 和 IO Connectors。

### 🎯 项目目标

- 为每篇技术文档提供可运行的代码示例
- 帮助开发者快速上手 Pulsar 的各项功能
- 提供本地开发环境的一键部署方案
- 展示 Pulsar 在不同编程语言中的最佳实践

### 📁 项目结构

```
apache-pulsar-examples/
├── java-examples/          # Java 客户端示例（20+ 个示例）
│   ├── quickstart/         # 快速入门：Producer/Consumer 基础
│   ├── producer-modes/     # Producer 模式：同步/异步/批量/带Key
│   ├── consumer-ack/       # Consumer ACK 机制
│   ├── key-shared-demo/    # Key_Shared 订阅
│   ├── subscription-modes/ # 订阅模式对比
│   ├── schema-registry/    # Schema 使用（Avro/Protobuf/JSON）
│   ├── transactions/       # 事务消息
│   ├── delayed-messages/   # 延迟消息
│   ├── dead-letter-topic/  # 死信队列（DLQ）
│   └── ...                 # 更多示例
├── python-examples/        # Python 客户端示例（18+ 个示例）
│   ├── quickstart/         # 快速入门
│   ├── producer-modes/     # Producer 模式
│   ├── consumer-ack/       # Consumer ACK 机制
│   ├── key-shared-demo/    # Key_Shared 订阅
│   ├── subscription-modes/ # 订阅模式
│   ├── schema-usage/       # Schema 使用
│   └── ...                 # 更多示例
├── go-examples/            # Go 客户端示例（15+ 个示例）
│   ├── quickstart/         # 快速入门
│   ├── producer-modes/     # Producer 模式
│   ├── consumer-ack/       # Consumer ACK 机制
│   └── ...                 # 更多示例
├── docker-compose/         # Docker Compose 配置
│   ├── docker-compose-standalone.yml  # 单机模式
│   ├── docker-compose-cluster.yml     # 集群模式
│   └── docker-compose-monitoring.yml  # 监控模式
├── functions/              # Pulsar Functions 示例
│   ├── java/               # Java Functions
│   ├── python/             # Python Functions
│   └── go/                 # Go Functions
├── connectors/             # IO Connectors 示例
│   ├── sources/            # Source Connectors
│   └── sinks/              # Sink Connectors
├── advanced-examples/      # 高级场景示例
├── projects/               # 实战项目示例
├── benchmarks/             # 性能测试示例
└── tests/                  # 测试框架
```

### 🚀 快速开始

#### 前置条件

- Docker 20.10+
- Docker Compose 2.0+
- JDK 11+（Java 示例）
- Python 3.7+（Python 示例）
- Go 1.19+（Go 示例）

#### 第一步：启动 Pulsar 环境

**方式 1：单机模式（推荐用于开发）**

```bash
cd docker-compose
docker-compose -f docker-compose-standalone.yml up -d
```

**方式 2：集群模式（用于生产环境模拟）**

```bash
cd docker-compose
docker-compose -f docker-compose-cluster.yml up -d
```

**方式 3：带监控（Prometheus + Grafana）**

```bash
cd docker-compose
docker-compose -f docker-compose-monitoring.yml up -d
```

等待服务启动完成（约2分钟）。验证服务状态：

```bash
docker exec pulsar bin/pulsar-admin brokers healthcheck
```

#### 第二步：运行第一个示例

**Java 示例：**

```bash
cd java-examples/quickstart
mvn clean package
mvn exec:java -Dexec.mainClass="com.example.pulsar.ProducerExample"
# 在另一个终端
mvn exec:java -Dexec.mainClass="com.example.pulsar.ConsumerExample"
```

**Python 示例：**

```bash
cd python-examples/quickstart
pip install -r requirements.txt
python producer.py
# 在另一个终端
python consumer.py
```

**Go 示例：**

```bash
cd go-examples/quickstart
go mod download
go run producer.go
# 在另一个终端
go run consumer.go
```

### 📚 示例分类

#### 基础示例

| 类别 | Java | Python | Go | 说明 |
|------|------|--------|----|----- |
| 快速入门 | ✅ | ✅ | ✅ | 基础 Producer/Consumer |
| Producer 模式 | ✅ | ✅ | ✅ | 同步/异步/批量/带Key |
| Consumer ACK | ✅ | ✅ | ✅ | Individual/Cumulative/Negative ACK |
| 订阅模式 | ✅ | ✅ | ✅ | Exclusive/Shared/Failover/Key_Shared |

#### 高级示例

| 类别 | Java | Python | Go | 说明 |
|------|------|--------|----|----- |
| Schema Registry | ✅ | ✅ | ✅ | Avro/Protobuf/JSON Schema |
| 事务消息 | ✅ | ⏳ | ⏳ | Exactly-Once 语义 |
| 延迟消息 | ✅ | ✅ | ✅ | 订单超时场景 |
| 死信队列 | ✅ | ✅ | ✅ | 失败消息处理 |
| 消息去重 | ✅ | ✅ | ⏳ | 防止重复消息 |

#### Pulsar Functions

| 类别 | Java | Python | Go | 说明 |
|------|------|--------|----|----- |
| 单词计数 | ✅ | ⏳ | ⏳ | 经典 MapReduce 示例 |
| 数据增强 | ✅ | ⏳ | ⏳ | 查询外部数据源 |
| 情感分析 | ⏳ | ✅ | ⏳ | NLP 处理 |
| 日志解析 | ⏳ | ⏳ | ✅ | 日志解析 |

#### IO Connectors

| 类型 | Connector | 状态 | 说明 |
|------|-----------|------|------|
| Source | File Source | ✅ | 读取本地文件 |
| Source | JDBC Source | ✅ | MySQL CDC |
| Source | Kafka Source | ✅ | Kafka 迁移 |
| Sink | Elasticsearch Sink | ✅ | 日志存储 |
| Sink | JDBC Sink | ✅ | 写入 PostgreSQL |
| Sink | Kafka Sink | ✅ | 双向同步 |

#### 实战项目

| 项目 | 状态 | 说明 |
|------|------|------|
| 实时用户行为分析 | ⏳ | 用户行为分析 pipeline |
| 金融支付系统 | ⏳ | 使用事务保证一致性 |
| IoT 消息接入平台 | ⏳ | 海量设备消息接入 |
| 日志聚合系统 | ⏳ | 中心化日志收集 |
| 实时风控引擎 | ⏳ | 基于 Functions 的风控 |

图例：✅ 已完成 | ⏳ 进行中 | ❌ 计划中

### 📖 文章映射

查看 [ARTICLE_MAPPING.md](ARTICLE_MAPPING.md) 了解文章与代码示例的完整映射关系。

### 🔧 开发环境

#### Java

- **JDK**: 11+
- **构建工具**: Maven 3.6+
- **Pulsar 客户端**: 3.2.0
- **IDE**: IntelliJ IDEA / Eclipse

#### Python

- **Python**: 3.7+
- **包管理器**: pip
- **Pulsar 客户端**: pulsar-client 3.2.0
- **IDE**: PyCharm / VS Code

#### Go

- **Go**: 1.19+
- **包管理器**: go mod
- **Pulsar 客户端**: github.com/apache/pulsar-client-go
- **IDE**: GoLand / VS Code

### 🧪 测试

运行所有测试：

```bash
# Java 示例
./test-build.sh java

# Python 示例
./test-build.sh python

# Go 示例
./test-build.sh go

# 集成测试
./test-integration.sh
```

### 🤝 贡献

欢迎贡献！请查看 [CONTRIBUTING.md](CONTRIBUTING.md) 了解详情。

### 📄 许可证

本项目采用 Apache License 2.0 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

### 🔗 相关资源

- [Apache Pulsar 官方文档](https://pulsar.apache.org/docs/)
- [Pulsar 技术文档系列（中文）](../Pulsar/)
- [GitHub 仓库](https://github.com/pingxin403/apache-pulsar-examples)
- [问题追踪](https://github.com/pingxin403/apache-pulsar-examples/issues)

### 📞 支持

- GitHub Issues: [报告问题](https://github.com/pingxin403/apache-pulsar-examples/issues/new)
- Discussions: [提问](https://github.com/pingxin403/apache-pulsar-examples/discussions)
