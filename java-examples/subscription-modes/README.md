# Subscription Modes Example

This example demonstrates the four subscription types in Apache Pulsar:
- **Exclusive**: Only one consumer can subscribe
- **Shared**: Multiple consumers share the load (round-robin)
- **Failover**: One active consumer, others standby
- **Key_Shared**: Parallel processing with ordering per key

## Prerequisites

- Pulsar running locally (see `../../docker-compose/`)
- JDK 11+
- Maven 3.6+

## Build

```bash
mvn clean package
```

## Run Examples

### 1. Exclusive Subscription

Only ONE consumer can use this subscription at a time.

```bash
mvn exec:java -Dexec.mainClass="com.example.pulsar.ExclusiveSubscriptionExample"
```

**Use cases**: Single consumer processing, strict ordering required

### 2. Shared Subscription

Multiple consumers share the workload in round-robin fashion.

```bash
# Terminal 1
mvn exec:java -Dexec.mainClass="com.example.pulsar.SharedSubscriptionExample"

# Terminal 2 (run simultaneously)
mvn exec:java -Dexec.mainClass="com.example.pulsar.SharedSubscriptionExample"
```

**Use cases**: High throughput, no ordering required, load balancing

### 3. Failover Subscription

One active consumer, others are standby. Automatic failover on failure.

```bash
# Terminal 1 (active)
mvn exec:java -Dexec.mainClass="com.example.pulsar.FailoverSubscriptionExample"

# Terminal 2 (standby)
mvn exec:java -Dexec.mainClass="com.example.pulsar.FailoverSubscriptionExample"
```

**Use cases**: High availability, ordered processing, automatic failover

### 4. Key_Shared Subscription

Multiple consumers process in parallel, but same key always goes to same consumer.

```bash
# Terminal 1
mvn exec:java -Dexec.mainClass="com.example.pulsar.KeySharedSubscriptionExample"

# Terminal 2 (run simultaneously)
mvn exec:java -Dexec.mainClass="com.example.pulsar.KeySharedSubscriptionExample"
```

**Use cases**: Parallel processing with per-key ordering (e.g., user sessions, order processing)

## Comparison Table

| Feature | Exclusive | Shared | Failover | Key_Shared |
|---------|-----------|--------|----------|------------|
| Consumers | 1 | Multiple | Multiple (1 active) | Multiple |
| Ordering | ✅ Global | ❌ None | ✅ Global | ✅ Per Key |
| Throughput | Low | High | Low | High |
| Failover | ❌ Manual | ✅ Auto | ✅ Auto | ✅ Auto |
| Use Case | Simple | Load Balance | HA | Parallel + Order |

## Testing with Producer

First, send some test messages:

```bash
# Send messages without keys (for Exclusive/Shared/Failover)
docker exec pulsar-standalone bin/pulsar-client produce \
  persistent://public/default/subscription-demo \
  --messages "Message-1,Message-2,Message-3,Message-4,Message-5" \
  --num-produce 1

# Send messages with keys (for Key_Shared)
docker exec pulsar-standalone bin/pulsar-client produce \
  persistent://public/default/subscription-demo \
  --messages "Order-A-1,Order-A-2,Order-B-1,Order-B-2,Order-A-3" \
  --key "orderA,orderA,orderB,orderB,orderA" \
  --num-produce 1
```

## Related Articles

- [统一消息模型](https://github.com/pingxin403/apache-pulsar-examples/tree/main/Pulsar/01-入门篇/04-统一消息模型.md)
