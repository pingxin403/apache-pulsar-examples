# Changelog

All notable changes to the Apache Pulsar Examples project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- GitHub Actions CI/CD 配置 (.github/workflows/ci.yml)
- 自动化 Java 编译测试（Maven）
- 自动化 Python 语法检查（flake8，支持 Python 3.8-3.11）
- 自动化 Go 编译和格式检查
- 集成测试自动化（Java quickstart 示例）
- CI 工作流程文档 (.github/workflows/README.md)
- CI 状态徽章支持

### Fixed
- Java key-shared-demo 实现已验证完整

### Changed
- 初始项目结构和文档
- Docker Compose environment for local development
- Java examples for core Pulsar features
- Python examples for core Pulsar features
- Go examples for core Pulsar features
- Comprehensive README with article mapping
- Build and integration test scripts
- Contributing guidelines

## [0.1.0] - 2024-01-XX

### Added

#### Project Infrastructure
- Project README with quick start guide and directory structure
- Apache License 2.0
- .gitignore for Java, Python, and Go projects
- CONTRIBUTING.md with contribution guidelines
- Docker Compose configuration for Pulsar Standalone mode

#### Java Examples
- **Quickstart**: Basic producer and consumer examples
- **Producer Modes**: Synchronous, asynchronous, batch, and key-based producers
- **Consumer ACK**: Individual, cumulative, and negative acknowledgment examples
- **Subscription Modes**: Exclusive, shared, failover, and key_shared subscriptions
- **Schema Registry**: Avro, Protobuf, and JSON schema examples
- **Transactions**: Transaction producer and consumer with exactly-once semantics
- **Delayed Messages**: Delayed message delivery example
- **Dead Letter Topic**: DLQ mechanism for failed message handling
- **Key_Shared Demo**: Key_shared subscription demonstration

#### Python Examples
- **Quickstart**: Basic producer and consumer examples
- **Producer Modes**: Synchronous, asynchronous, and batch producers
- **Consumer ACK**: Individual, cumulative, and negative acknowledgment examples
- **Subscription Modes**: Exclusive, shared, failover, and key_shared subscriptions
- **Schema Usage**: Avro and JSON schema examples
- **Async Operations**: Asynchronous producer and consumer examples
- **Key_Shared Demo**: Key_shared subscription demonstration

#### Go Examples
- **Quickstart**: Basic producer and consumer examples
- **Producer Modes**: Synchronous, asynchronous, and batch producers
- **Consumer ACK**: Individual, cumulative, and negative acknowledgment examples

#### Documentation
- Comprehensive README for each example with:
  - Feature description
  - Prerequisites
  - Setup instructions
  - Running instructions
  - Expected output
  - Code explanation
  - Related articles
- Article-to-example mapping table in main README
- Docker Compose setup guide

#### Testing
- `test-build.sh`: Automated build testing for all examples
- `test-integration.sh`: Integration testing with Docker Compose environment

### Dependencies
- Apache Pulsar: 3.2.0
- Java: JDK 11+
- Python: 3.7+
- Go: 1.19+
- Maven: 3.6+
- Docker: 20.10+
- Docker Compose: 2.0+

## Version History

### Version Numbering

This project follows semantic versioning:
- **MAJOR**: Incompatible API changes or major restructuring
- **MINOR**: New examples or features in a backwards-compatible manner
- **PATCH**: Bug fixes and documentation improvements

### Planned Releases

#### v0.2.0 (Planned)
- Additional Java advanced examples
- Additional Python advanced examples
- Additional Go examples
- Pulsar Functions examples (Java, Python, Go)
- IO Connectors examples (Sources and Sinks)

#### v0.3.0 (Planned)
- Complete project examples (real-world scenarios)
- Advanced configuration examples
- Performance benchmarking examples
- Monitoring and observability examples

#### v1.0.0 (Planned)
- Complete coverage of all 77 technical articles
- Full test coverage
- CI/CD integration with GitHub Actions
- Multi-language documentation (English and Chinese)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute to this project.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Apache Pulsar community for the excellent messaging platform
- Contributors who have helped improve these examples
- Technical documentation authors whose articles these examples support

---

**Note**: This changelog will be updated with each release. For the latest changes, see the [Unreleased] section above.
