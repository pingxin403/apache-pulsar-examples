#!/bin/bash
# Test script to compile all Java, Python, and Go examples
# This script validates that all code examples can be successfully built

set -e  # Exit on first error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Testing Apache Pulsar Examples Build"
echo "=========================================="
echo ""

# Track results
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

# Test Java examples
echo "=========================================="
echo "Testing Java Examples Compilation"
echo "=========================================="
echo ""

if command -v mvn &> /dev/null; then
    for dir in java-examples/*/; do
        if [ -f "$dir/pom.xml" ]; then
            example_name=$(basename "$dir")
            echo "Building Java example: $example_name"
            
            cd "$dir"
            if mvn clean package -q -DskipTests 2>&1 | grep -q "BUILD SUCCESS"; then
                print_result "Java: $example_name" 0
            else
                print_result "Java: $example_name" 1
            fi
            cd "$SCRIPT_DIR"
            echo ""
        fi
    done
else
    echo -e "${YELLOW}WARNING${NC}: Maven not found, skipping Java examples"
    echo ""
fi

# Test Python examples
echo "=========================================="
echo "Testing Python Examples Syntax"
echo "=========================================="
echo ""

if command -v python3 &> /dev/null; then
    for dir in python-examples/*/; do
        example_name=$(basename "$dir")
        echo "Checking Python example: $example_name"
        
        # Check all Python files in the directory
        py_files=$(find "$dir" -maxdepth 1 -name "*.py" 2>/dev/null)
        
        if [ -n "$py_files" ]; then
            all_passed=true
            for py_file in $py_files; do
                if ! python3 -m py_compile "$py_file" 2>/dev/null; then
                    all_passed=false
                    break
                fi
            done
            
            if [ "$all_passed" = true ]; then
                print_result "Python: $example_name" 0
            else
                print_result "Python: $example_name" 1
            fi
        else
            echo -e "${YELLOW}WARNING${NC}: No Python files found in $example_name"
        fi
        echo ""
    done
else
    echo -e "${YELLOW}WARNING${NC}: Python3 not found, skipping Python examples"
    echo ""
fi

# Test Go examples
echo "=========================================="
echo "Testing Go Examples Compilation"
echo "=========================================="
echo ""

if command -v go &> /dev/null; then
    for dir in go-examples/*/; do
        if [ -f "$dir/go.mod" ]; then
            example_name=$(basename "$dir")
            echo "Building Go example: $example_name"
            
            cd "$dir"
            if go build ./... 2>&1; then
                print_result "Go: $example_name" 0
            else
                print_result "Go: $example_name" 1
            fi
            cd "$SCRIPT_DIR"
            echo ""
        fi
    done
else
    echo -e "${YELLOW}WARNING${NC}: Go not found, skipping Go examples"
    echo ""
fi

# Print summary
echo "=========================================="
echo "Build Test Summary"
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
    echo -e "${RED}BUILD TESTS FAILED${NC}"
    exit 1
else
    echo -e "${GREEN}ALL BUILD TESTS PASSED${NC}"
    exit 0
fi
