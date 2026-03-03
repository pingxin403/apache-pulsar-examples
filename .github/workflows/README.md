# GitHub Actions CI/CD

本目录包含 Apache Pulsar Examples 项目的持续集成配置。

## CI 工作流程

### ci.yml - 持续集成

自动化测试所有代码示例，确保它们始终可编译和运行。

**触发条件**:
- Push 到 `main` 或 `develop` 分支
- Pull Request 到 `main` 分支

**测试任务**:

1. **Build Java Examples** 🔨
   - 使用 JDK 11 和 Maven
   - 编译所有 Java 示例项目
   - 验证 `mvn clean package` 成功

2. **Check Python Examples** 🐍
   - 测试 Python 3.8, 3.9, 3.10, 3.11 兼容性
   - 使用 flake8 进行语法检查
   - 验证代码质量标准

3. **Build Go Examples** 🔧
   - 使用 Go 1.21
   - 编译所有 Go 示例
   - 检查代码格式（gofmt）

4. **Integration Tests** 🧪
   - 启动 Pulsar Standalone 环境
   - 运行 Java quickstart 示例
   - 验证 Producer 和 Consumer 正常工作

5. **CI Summary** 📊
   - 汇总所有测试结果
   - 如果任何测试失败，整个 CI 失败

## 本地测试

在提交代码前，可以使用以下脚本在本地运行测试：

```bash
# 编译测试
./test-build.sh

# 集成测试
./test-integration.sh
```

## 状态徽章

在项目 README 中添加 CI 状态徽章：

```markdown
![CI](https://github.com/pingxin403/apache-pulsar-examples/workflows/CI/badge.svg)
```

## 故障排查

### Java 编译失败
- 检查 pom.xml 依赖版本
- 确保使用 JDK 11+
- 运行 `mvn clean install` 清理缓存

### Python 检查失败
- 运行 `flake8 python-examples/` 查看具体错误
- 修复代码格式问题
- 确保行长度不超过 120 字符

### Go 编译失败
- 运行 `go mod tidy` 更新依赖
- 运行 `gofmt -w .` 格式化代码
- 检查 go.mod 中的依赖版本

### 集成测试失败
- 确保 Docker 正常运行
- 检查 Pulsar 容器日志
- 验证端口 6650 和 8080 未被占用

## 扩展 CI

要添加新的测试步骤：

1. 编辑 `.github/workflows/ci.yml`
2. 添加新的 job 或 step
3. 确保新步骤有清晰的名称和错误处理
4. 测试 CI 配置后提交

## 相关文档

- [GitHub Actions 文档](https://docs.github.com/en/actions)
- [Maven CI 最佳实践](https://maven.apache.org/guides/introduction/introduction-to-the-lifecycle.html)
- [Python flake8 配置](https://flake8.pycqa.org/en/latest/)
- [Go 测试指南](https://golang.org/doc/code.html#Testing)
