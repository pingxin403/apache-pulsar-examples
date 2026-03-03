# Docker Compose for Apache Pulsar

This directory contains Docker Compose configuration for running Apache Pulsar locally.

## Quick Start

### Start Pulsar Standalone

```bash
docker-compose up -d
```

### Verify Pulsar is Running

Wait for about 30 seconds for Pulsar to start, then verify:

```bash
docker exec pulsar-standalone bin/pulsar-admin brokers healthcheck
```

You should see: `ok`

### Check Pulsar Status

```bash
docker-compose ps
```

### View Pulsar Logs

```bash
docker-compose logs -f pulsar
```

### Stop Pulsar

```bash
docker-compose down
```

### Stop and Remove Data

```bash
docker-compose down -v
```

## Access Points

- **Pulsar Service URL**: `pulsar://localhost:6650`
- **Pulsar HTTP Service**: `http://localhost:8080`
- **Pulsar Admin REST API**: `http://localhost:8080/admin/v2`

## Test Connection

### Using pulsar-admin (inside container)

```bash
# Create a topic
docker exec pulsar-standalone bin/pulsar-admin topics create persistent://public/default/test-topic

# List topics
docker exec pulsar-standalone bin/pulsar-admin topics list public/default

# Send a test message
docker exec pulsar-standalone bin/pulsar-client produce persistent://public/default/test-topic --messages "Hello Pulsar"

# Consume messages
docker exec pulsar-standalone bin/pulsar-client consume persistent://public/default/test-topic --subscription-name test-sub --num-messages 1
```

## Configuration

The Pulsar standalone instance is configured with:
- **Memory**: 512MB heap, 256MB direct memory
- **Data persistence**: Stored in Docker volume `pulsar-data`
- **Health check**: Automatic health monitoring

## Troubleshooting

### Container won't start

Check logs:
```bash
docker-compose logs pulsar
```

### Port already in use

If ports 6650 or 8080 are already in use, modify the `docker-compose.yml` file:

```yaml
ports:
  - "16650:6650"  # Change external port
  - "18080:8080"  # Change external port
```

Then update your client connection URL accordingly.

### Reset everything

```bash
docker-compose down -v
docker-compose up -d
```

## Next Steps

Once Pulsar is running, you can:
1. Run Java examples: `cd ../java-examples/quickstart`
2. Run Python examples: `cd ../python-examples/quickstart`
3. Run Go examples: `cd ../go-examples/quickstart`
