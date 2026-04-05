#!/bin/bash
# Elasticsearch Sink Connector 部署脚本

set -e

echo "🔍 启动 Elasticsearch..."
docker-compose up -d

echo "⏳ 等待 Elasticsearch 就绪..."
until curl -s http://localhost:9200/_cluster/health > /dev/null 2>&1; do
    sleep 2
done
echo "   ✅ Elasticsearch 已就绪"

echo ""
echo "🚀 部署 Elasticsearch Sink Connector..."
docker exec pulsar bin/pulsar-admin sinks create \
    --tenant public \
    --namespace default \
    --name elasticsearch-sink \
    --sink-type elastic_search \
    --inputs persistent://public/default/es-sink-input \
    --sink-config '{
        "elasticSearchUrl": "http://host.docker.internal:9200",
        "indexName": "pulsar-messages",
        "schemaEnable": false,
        "bulkEnabled": true,
        "bulkActions": 100
    }'

echo "✅ Elasticsearch Sink Connector 部署成功"
echo ""
echo "📊 查看状态:"
echo "  docker exec pulsar bin/pulsar-admin sinks status --name elasticsearch-sink"
echo ""
echo "📤 测试发送消息:"
echo '  docker exec pulsar bin/pulsar-client produce es-sink-input --messages '"'"'{"name":"test","value":42}'"'"''
echo ""
echo "🔍 查看 Elasticsearch 数据:"
echo "  curl http://localhost:9200/pulsar-messages/_search?pretty"
echo ""
echo "🖥️ Kibana 控制台: http://localhost:5601"
