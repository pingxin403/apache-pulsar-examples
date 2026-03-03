#!/bin/bash
# Integration test script for Apache Pulsar examples
# This script starts a Pulsar environment and runs integration tests

set -e  # Exit on first error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
DOCKER_COMPOSE_FILE="docker-compose/docker-compose.yml"
PULSAR_CONTAINER="pulsar-standalone"
HEALTH_CHECK_TIMEOUT=120
TEST_TIMEOUT=30

echo "=========================================="
echo "Apache Pulsar Integration Tests"
echo "=========================================="
echo ""

# Check prerequisites
echo "Checking prerequisites..."
if ! command -v docker &> /dev/null; then
    echo -e "${RED}ERROR${NC}: Docker is not installed"
    exit 1
fi

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo -e "${RED}ERROR${NC}: Docker Compose is not installed"
    exit 1
fi

# Determine docker compose command
if docker compose version &> /dev/null 2>&1; then
    DOCKER_COMPOSE_CMD="docker compose"
else
    DOCKER_COMPOSE_CMD="docker-compose"
fi

echo -e "${GREEN}✓${NC} Prerequisites check passed"
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up..."
    cd "$SCRIPT_DIR"
    $DOCKER_COMPOSE_CMD -f "$DOCKER_COMPOSE_FILE" down -v 2>/dev/null || true
    echo "Cleanup completed"
}

# Set trap to cleanup on exit
trap cleanup EXIT INT TERM

# Start Pulsar environment
echo "=========================================="
echo "Starting Pulsar Environment"
echo "=========================================="
echo ""

cd "$SCRIPT_DIR"
echo "Using Docker Compose file: $DOCKER_COMPOSE_FILE"
$DOCKER_COMPOSE_CMD -f "$DOCKER_COMPOSE_FILE" up -d

echo ""
echo "Waiting for Pulsar to be ready (timeout: ${HEALTH_CHECK_TIMEOUT}s)..."

# Wait for Pulsar to be healthy
start_time=$(date +%s)
while true; do
    current_time=$(date +%s)
    elapsed=$((current_time - start_time))
    
    if [ $elapsed -ge $HEALTH_CHECK_TIMEOUT ]; then
        echo -e "${RED}ERROR${NC}: Pulsar failed to start within ${HEALTH_CHECK_TIMEOUT} seconds"
        exit 1
    fi
    
    # Check if container is running and healthy
    if docker exec $PULSAR_CONTAINER bin/pulsar-admin brokers healthcheck 2>/dev/null; then
        echo -e "${GREEN}✓${NC} Pulsar is ready!"
        break
    fi
    
    echo -n "."
    sleep 2
done

echo ""
echo ""

# Run integration tests
echo "=========================================="
echo "Running Integration Tests"
echo "=========================================="
echo ""

TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Function to print test result
print_result() {
    local test_name=$1
    local result=$2
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    if [ "$result" -eq 0 ]; then
        echo -e "${GREEN}✓ PASSED${NC}: $test_name"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        echo -e "${RED}✗ FAILED${NC}: $test_name"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
}

# Test 1: Java Quickstart Example
echo -e "${BLUE}Test 1: Java Quickstart Example${NC}"
if [ -d "java-examples/quickstart" ] && [ -f "java-examples/quickstart/pom.xml" ]; then
    cd java-examples/quickstart
    
    # Build the example
    if mvn clean package -q -DskipTests 2>&1 | grep -q "BUILD SUCCESS"; then
        # Run producer in background
        timeout $TEST_TIMEOUT mvn exec:java -Dexec.mainClass="com.example.pulsar.ProducerExample" -q > /tmp/producer.log 2>&1 &
        PRODUCER_PID=$!
        sleep 3
        
        # Run consumer in background
        timeout $TEST_TIMEOUT mvn exec:java -Dexec.mainClass="com.example.pulsar.ConsumerExample" -q > /tmp/consumer.log 2>&1 &
        CONSUMER_PID=$!
        sleep 5
        
        # Check if processes ran successfully
        if grep -q "Message sent" /tmp/producer.log 2>/dev/null && grep -q "Received message" /tmp/consumer.log 2>/dev/null; then
            print_result "Java Quickstart" 0
        else
            print_result "Java Quickstart" 1
        fi
        
        # Cleanup processes
        kill $PRODUCER_PID $CONSUMER_PID 2>/dev/null || true
    else
        print_result "Java Quickstart (build failed)" 1
    fi
    
    cd "$SCRIPT_DIR"
else
    echo -e "${YELLOW}SKIPPED${NC}: Java quickstart example not found"
fi
echo ""

# Test 2: Python Quickstart Example
echo -e "${BLUE}Test 2: Python Quickstart Example${NC}"
if [ -d "python-examples/quickstart" ]; then
    cd python-examples/quickstart
    
    # Install dependencies if requirements.txt exists
    if [ -f "requirements.txt" ]; then
        pip install -q -r requirements.txt 2>/dev/null || true
    fi
    
    # Run producer in background
    if [ -f "producer.py" ]; then
        timeout $TEST_TIMEOUT python3 producer.py > /tmp/py_producer.log 2>&1 &
        PY_PRODUCER_PID=$!
        sleep 3
        
        # Run consumer in background
        if [ -f "consumer.py" ]; then
            timeout $TEST_TIMEOUT python3 consumer.py > /tmp/py_consumer.log 2>&1 &
            PY_CONSUMER_PID=$!
            sleep 5
            
            # Check if processes ran successfully
            if grep -q "Sent message" /tmp/py_producer.log 2>/dev/null && grep -q "Received message" /tmp/py_consumer.log 2>/dev/null; then
                print_result "Python Quickstart" 0
            else
                print_result "Python Quickstart" 1
            fi
            
            # Cleanup processes
            kill $PY_PRODUCER_PID $PY_CONSUMER_PID 2>/dev/null || true
        else
            print_result "Python Quickstart (consumer.py not found)" 1
        fi
    else
        print_result "Python Quickstart (producer.py not found)" 1
    fi
    
    cd "$SCRIPT_DIR"
else
    echo -e "${YELLOW}SKIPPED${NC}: Python quickstart example not found"
fi
echo ""

# Test 3: Go Quickstart Example
echo -e "${BLUE}Test 3: Go Quickstart Example${NC}"
if [ -d "go-examples/quickstart" ] && [ -f "go-examples/quickstart/go.mod" ]; then
    cd go-examples/quickstart
    
    # Build the examples
    if go build -o producer producer.go 2>/dev/null && go build -o consumer consumer.go 2>/dev/null; then
        # Run producer in background
        timeout $TEST_TIMEOUT ./producer > /tmp/go_producer.log 2>&1 &
        GO_PRODUCER_PID=$!
        sleep 3
        
        # Run consumer in background
        timeout $TEST_TIMEOUT ./consumer > /tmp/go_consumer.log 2>&1 &
        GO_CONSUMER_PID=$!
        sleep 5
        
        # Check if processes ran successfully
        if grep -q "Message published" /tmp/go_producer.log 2>/dev/null && grep -q "Received message" /tmp/go_consumer.log 2>/dev/null; then
            print_result "Go Quickstart" 0
        else
            print_result "Go Quickstart" 1
        fi
        
        # Cleanup processes
        kill $GO_PRODUCER_PID $GO_CONSUMER_PID 2>/dev/null || true
        rm -f producer consumer
    else
        print_result "Go Quickstart (build failed)" 1
    fi
    
    cd "$SCRIPT_DIR"
else
    echo -e "${YELLOW}SKIPPED${NC}: Go quickstart example not found"
fi
echo ""

# Test 4: Pulsar Admin Commands
echo -e "${BLUE}Test 4: Pulsar Admin Commands${NC}"
if docker exec $PULSAR_CONTAINER bin/pulsar-admin tenants list 2>/dev/null | grep -q "public"; then
    print_result "Pulsar Admin Commands" 0
else
    print_result "Pulsar Admin Commands" 1
fi
echo ""

# Print summary
echo "=========================================="
echo "Integration Test Summary"
echo "=========================================="
echo "Total tests: $TOTAL_TESTS"
echo -e "${GREEN}Passed: $PASSED_TESTS${NC}"
if [ $FAILED_TESTS -gt 0 ]; then
    echo -e "${RED}Failed: $FAILED_TESTS${NC}"
else
    echo "Failed: $FAILED_TESTS"
fi
echo ""

# Exit with appropriate code
if [ $FAILED_TESTS -gt 0 ]; then
    echo -e "${RED}INTEGRATION TESTS FAILED${NC}"
    exit 1
else
    echo -e "${GREEN}ALL INTEGRATION TESTS PASSED${NC}"
    exit 0
fi
