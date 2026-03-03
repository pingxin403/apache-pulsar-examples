# Java Quickstart Example

This example demonstrates the basic usage of Apache Pulsar with Java, including:
- Creating a Producer to send messages
- Creating a Consumer to receive messages
- Proper resource management with try-with-resources

## Prerequisites

- JDK 11 or higher
- Maven 3.6+
- Pulsar running locally (see `../../docker-compose/`)

## Build

```bash
mvn clean package
```

## Run

### 1. Start Pulsar (if not already running)

```bash
cd ../../docker-compose
docker-compose up -d
```

### 2. Run Producer

In one terminal:

```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.ProducerExample"
```

Expected output:
```
Sent: Hello Pulsar! Message 0
Sent: Hello Pulsar! Message 1
...
Successfully sent 10 messages!
```

### 3. Run Consumer

In another terminal:

```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.ConsumerExample"
```

Expected output:
```
Waiting for messages...

Received: Hello Pulsar! Message 0
Received: Hello Pulsar! Message 1
...
Successfully received 10 messages!
```

## Code Explanation

### ProducerExample.java

- **PulsarClient**: Creates a connection to Pulsar broker
- **Producer**: Sends messages to a topic
- **try-with-resources**: Automatically closes client and producer

Key points:
- Service URL: `pulsar://localhost:6650`
- Topic: `persistent://public/default/quickstart-topic`
- Sends 10 text messages

### ConsumerExample.java

- **Consumer**: Receives messages from a topic
- **Subscription**: Named subscription for message tracking
- **Acknowledge**: Confirms message processing

Key points:
- Subscription type: Exclusive (only one consumer)
- Acknowledges each message after processing
- Uses negative acknowledgment for failures

## Troubleshooting

### Connection refused

Make sure Pulsar is running:
```bash
docker exec pulsar-standalone bin/pulsar-admin brokers healthcheck
```

### Build fails

Check Java version:
```bash
java -version  # Should be 11+
mvn -version   # Should be 3.6+
```

## Next Steps

- Explore [Producer Modes](../producer-modes/) for different sending patterns
- Learn about [Consumer ACK](../consumer-ack/) mechanisms
- Try [Key_Shared](../key-shared-demo/) subscription for parallel processing

## Related Articles

- [5分钟上手Pulsar](https://github.com/pingxin403/apache-pulsar-examples/tree/main/Pulsar/01-入门篇/01-5分钟上手Pulsar.md)
