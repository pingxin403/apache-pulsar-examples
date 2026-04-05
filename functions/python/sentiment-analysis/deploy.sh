#!/bin/bash
# Sentiment Analysis Function 部署脚本

set -e

echo "🚀 部署 Sentiment Analysis Function 到 Pulsar..."

docker exec pulsar bin/pulsar-admin functions create \
    --tenant public \
    --namespace default \
    --name sentiment-analysis \
    --py /pulsar/functions/sentiment_function.py \
    --classname sentiment_function.SentimentFunction \
    --inputs persistent://public/default/reviews \
    --output persistent://public/default/sentiment-results \
    --auto-ack true

echo "✅ Function 部署成功"
echo ""
echo "📊 查看 Function 状态:"
echo "  docker exec pulsar bin/pulsar-admin functions status --name sentiment-analysis"
echo ""
echo "📤 测试发送消息:"
echo "  docker exec pulsar bin/pulsar-client produce reviews --messages 'This product is great and amazing'"
echo ""
echo "📥 查看输出:"
echo "  docker exec pulsar bin/pulsar-client consume sentiment-results -s test -n 1"
