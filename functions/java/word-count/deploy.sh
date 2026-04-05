#!/bin/bash
# Word Count Function 部署脚本

set -e

echo "🔨 编译 Word Count Function..."
mvn clean package -q

JAR_FILE="target/word-count-function-1.0.0.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "❌ 编译失败，JAR 文件不存在"
    exit 1
fi

echo "🚀 部署 Function 到 Pulsar..."
docker exec pulsar bin/pulsar-admin functions create \
    --function-config-file /dev/stdin \
    --jar "/pulsar/functions/$JAR_FILE" <<EOF
tenant: public
namespace: default
name: word-count
className: com.example.pulsar.WordCountFunction
inputs:
  - persistent://public/default/sentences
output: persistent://public/default/word-counts
autoAck: true
parallelism: 1
EOF

echo "✅ Function 部署成功"
echo ""
echo "📊 查看 Function 状态:"
echo "  docker exec pulsar bin/pulsar-admin functions status --name word-count"
echo ""
echo "📤 测试发送消息:"
echo "  docker exec pulsar bin/pulsar-client produce sentences --messages 'hello world hello pulsar'"
echo ""
echo "📥 查看输出:"
echo "  docker exec pulsar bin/pulsar-client consume word-counts -s test -n 1"
