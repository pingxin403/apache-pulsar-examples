#!/bin/bash
# File Source Connector 部署脚本

set -e

echo "📁 创建文件目录..."
docker exec pulsar mkdir -p /tmp/pulsar-file-source/input
docker exec pulsar mkdir -p /tmp/pulsar-file-source/processed

echo "🚀 部署 File Source Connector..."
docker exec pulsar bin/pulsar-admin sources create \
    --tenant public \
    --namespace default \
    --name file-source \
    --source-type file \
    --destination-topic-name persistent://public/default/file-source-output \
    --source-config '{
        "inputDirectory": "/tmp/pulsar-file-source/input",
        "recurse": true
    }'

echo "✅ File Source Connector 部署成功"
echo ""
echo "📊 查看状态:"
echo "  docker exec pulsar bin/pulsar-admin sources status --name file-source"
echo ""
echo "📝 测试 - 创建测试文件:"
echo "  docker exec pulsar bash -c 'echo \"Hello from file source\" > /tmp/pulsar-file-source/input/test.txt'"
echo ""
echo "📥 查看输出:"
echo "  docker exec pulsar bin/pulsar-client consume file-source-output -s test -n 1"
