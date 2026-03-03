# Contributing to Apache Pulsar Examples

Thank you for your interest in contributing to the Apache Pulsar Examples project! This document provides guidelines for contributing new code examples and improving existing ones.

## Table of Contents

- [Getting Started](#getting-started)
- [Project Structure](#project-structure)
- [Contributing Guidelines](#contributing-guidelines)
- [Code Example Standards](#code-example-standards)
- [Documentation Standards](#documentation-standards)
- [Testing Requirements](#testing-requirements)
- [Submission Process](#submission-process)

## Getting Started

### Prerequisites

Before contributing, ensure you have the following installed:

- **Java**: JDK 11 or higher
- **Python**: Python 3.7 or higher
- **Go**: Go 1.19 or higher
- **Maven**: Maven 3.6 or higher
- **Docker**: Docker 20.10 or higher
- **Docker Compose**: Docker Compose 2.0 or higher

### Setting Up Development Environment

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/apache-pulsar-examples.git
   cd apache-pulsar-examples
   ```

3. Start the local Pulsar environment:
   ```bash
   docker-compose -f docker-compose/docker-compose.yml up -d
   ```

4. Verify Pulsar is running:
   ```bash
   docker exec pulsar-standalone bin/pulsar-admin brokers healthcheck
   ```

## Project Structure

The project is organized by programming language and functionality:

```
apache-pulsar-examples/
├── java-examples/          # Java client examples
├── python-examples/        # Python client examples
├── go-examples/            # Go client examples
├── functions/              # Pulsar Functions examples
├── connectors/             # IO Connectors examples
├── advanced-examples/      # Advanced scenarios
├── projects/               # Complete project examples
└── docker-compose/         # Docker environment configs
```

## Contributing Guidelines

### Types of Contributions

We welcome the following types of contributions:

1. **New Code Examples**: Add examples for features not yet covered
2. **Bug Fixes**: Fix errors in existing examples
3. **Documentation Improvements**: Enhance README files and comments
4. **Test Improvements**: Add or improve test scripts
5. **Performance Optimizations**: Improve example efficiency

### Before You Start

1. Check existing issues and pull requests to avoid duplication
2. Open an issue to discuss major changes before implementing
3. Ensure your contribution aligns with the project's goals

## Code Example Standards

### General Requirements

All code examples must:

1. **Be Self-Contained**: Each example should run independently
2. **Use Default Configuration**: Connect to `pulsar://localhost:6650` by default
3. **Include Error Handling**: Properly handle exceptions and errors
4. **Release Resources**: Close clients and producers/consumers properly
5. **Include Logging**: Add informative log messages
6. **Be Well-Commented**: Explain key concepts and parameters

### Language-Specific Standards

#### Java Examples

- Use **Pulsar Client 3.2.0**
- Include `pom.xml` with all dependencies
- Follow Java naming conventions (CamelCase)
- Use try-with-resources for resource management
- Include proper exception handling

**Example Structure**:
```
example-name/
├── src/main/java/com/example/pulsar/
│   ├── ProducerExample.java
│   └── ConsumerExample.java
├── pom.xml
├── README.md
└── test.sh
```

**pom.xml Template**:
```xml
<dependency>
    <groupId>org.apache.pulsar</groupId>
    <artifactId>pulsar-client</artifactId>
    <version>3.2.0</version>
</dependency>
```

#### Python Examples

- Use **pulsar-client 3.2.0**
- Include `requirements.txt` with dependencies
- Follow PEP 8 style guidelines
- Use type hints where appropriate
- Include docstrings for functions and classes
- Use context managers for resource management

**Example Structure**:
```
example-name/
├── producer.py
├── consumer.py
├── requirements.txt
├── README.md
└── test.sh
```

**requirements.txt Template**:
```
pulsar-client==3.2.0
```

#### Go Examples

- Use latest **pulsar-client-go**
- Include `go.mod` and `go.sum`
- Follow Go naming conventions
- Use `context.Context` for lifecycle management
- Use `defer` for resource cleanup
- Format code with `gofmt`

**Example Structure**:
```
example-name/
├── producer.go
├── consumer.go
├── go.mod
├── go.sum
├── README.md
└── test.sh
```

### Code Quality Checklist

Before submitting, ensure your code:

- [ ] Compiles without errors
- [ ] Runs successfully in local Docker environment
- [ ] Includes comprehensive error handling
- [ ] Properly closes all resources
- [ ] Contains informative log messages
- [ ] Follows language-specific conventions
- [ ] Is well-commented and documented

## Documentation Standards

### README.md Requirements

Every example must include a `README.md` with:

1. **Title and Description**: Clear explanation of what the example demonstrates
2. **Prerequisites**: Required software and versions
3. **Setup Instructions**: How to prepare the environment
4. **Running the Example**: Step-by-step execution instructions
5. **Expected Output**: What users should see when running the example
6. **Code Explanation**: Key concepts and important code sections
7. **Related Articles**: Links to relevant documentation

### README.md Template

```markdown
# Example Name

Brief description of what this example demonstrates.

## Prerequisites

- Apache Pulsar 3.2.0 or higher
- [Language] [Version] or higher
- Docker and Docker Compose

## Setup

1. Start Pulsar environment:
   ```bash
   docker-compose -f ../../docker-compose/docker-compose.yml up -d
   ```

2. Install dependencies:
   ```bash
   # Language-specific installation commands
   ```

## Running the Example

1. Run the producer:
   ```bash
   # Producer command
   ```

2. Run the consumer:
   ```bash
   # Consumer command
   ```

## Expected Output

Producer output:
```
[Expected producer output]
```

Consumer output:
```
[Expected consumer output]
```

## Code Explanation

### Key Concepts

- **Concept 1**: Explanation
- **Concept 2**: Explanation

### Important Code Sections

[Explain important parts of the code]

## Related Articles

- [Article Title](link)

## Troubleshooting

Common issues and solutions.
```

### Chinese Documentation

For each `README.md`, provide a Chinese version `README_zh.md` with the same structure.

## Testing Requirements

### Build Testing

All examples must pass the build test:

```bash
./test-build.sh
```

This script:
- Compiles all Java examples with Maven
- Checks Python syntax with py_compile
- Compiles all Go examples with go build

### Integration Testing

Examples should be tested in a real Pulsar environment:

```bash
./test-integration.sh
```

This script:
- Starts Pulsar with Docker Compose
- Runs quickstart examples for each language
- Verifies message production and consumption

### Manual Testing

Before submitting:

1. Start the Pulsar environment
2. Run your example following the README instructions
3. Verify the output matches expectations
4. Test error scenarios (e.g., Pulsar not running)
5. Verify resource cleanup (no hanging processes)

## Submission Process

### 1. Create a Branch

```bash
git checkout -b feature/your-example-name
```

### 2. Implement Your Changes

- Add your code example
- Create comprehensive documentation
- Add or update tests

### 3. Test Your Changes

```bash
# Run build tests
./test-build.sh

# Run integration tests (if applicable)
./test-integration.sh
```

### 4. Commit Your Changes

Use clear, descriptive commit messages:

```bash
git add .
git commit -m "Add [language] example for [feature]

- Implement producer and consumer
- Add comprehensive README
- Include error handling and logging"
```

### 5. Push to Your Fork

```bash
git push origin feature/your-example-name
```

### 6. Create a Pull Request

1. Go to the original repository on GitHub
2. Click "New Pull Request"
3. Select your branch
4. Fill in the PR template with:
   - Description of changes
   - Related issues
   - Testing performed
   - Screenshots (if applicable)

### 7. Address Review Comments

- Respond to reviewer feedback promptly
- Make requested changes
- Push updates to your branch

## Code Review Process

All contributions go through code review:

1. **Automated Checks**: CI/CD runs build and test scripts
2. **Manual Review**: Maintainers review code quality and documentation
3. **Testing**: Reviewers may test examples locally
4. **Approval**: At least one maintainer approval required
5. **Merge**: Maintainers merge approved PRs

## Style Guidelines

### Code Style

- **Java**: Follow Oracle Java Code Conventions
- **Python**: Follow PEP 8
- **Go**: Follow Effective Go guidelines

### Commit Messages

- Use present tense ("Add feature" not "Added feature")
- Use imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit first line to 72 characters
- Reference issues and PRs when applicable

### Documentation Style

- Use clear, concise language
- Provide complete, runnable examples
- Include expected output
- Explain "why" not just "what"

## Getting Help

If you need help:

- Open an issue with the `question` label
- Join the Apache Pulsar Slack channel
- Check existing documentation and examples

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

## Recognition

Contributors will be recognized in:
- CHANGELOG.md for each release
- GitHub contributors page
- Project documentation

Thank you for contributing to Apache Pulsar Examples!
