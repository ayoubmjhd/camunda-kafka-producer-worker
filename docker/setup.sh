#!/bin/bash
# =============================================================================
# Setup script: Creates Kafka topics, registers Avro schemas, deploys BPMN
# Run this AFTER docker-compose up -d
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCHEMA_REGISTRY_URL="http://localhost:8081"
ZEEBE_ADDRESS="localhost:26500"

echo "============================================"
echo " Camunda Kafka Avro - Test Environment Setup"
echo "============================================"

# -----------------------------------------------
# 1. Wait for Schema Registry
# -----------------------------------------------
echo ""
echo "⏳ Waiting for Schema Registry to be ready..."
until curl -s "$SCHEMA_REGISTRY_URL/subjects" > /dev/null 2>&1; do
    echo "   Schema Registry not ready, retrying in 3s..."
    sleep 3
done
echo "✅ Schema Registry is ready!"

# -----------------------------------------------
# 2. Create Kafka topic
# -----------------------------------------------
echo ""
echo "📦 Creating Kafka topic 'orders'..."
docker exec kafka kafka-topics --create \
    --bootstrap-server localhost:29092 \
    --topic orders \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists \
    2>/dev/null && echo "✅ Topic 'orders' created!" || echo "ℹ️  Topic 'orders' already exists."

# -----------------------------------------------
# 3. Register Avro schemas
# -----------------------------------------------
echo ""
echo "📋 Registering Avro schema for subject 'orders-value'..."

SCHEMA_VAL=$(cat "$SCRIPT_DIR/test-data/order-schema.avsc" | python3 -c "
import sys, json
schema = json.load(sys.stdin)
payload = {'schema': json.dumps(schema), 'schemaType': 'AVRO'}
print(json.dumps(payload))
")

RESPONSE_VAL=$(curl -s -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "$SCHEMA_VAL" \
    "$SCHEMA_REGISTRY_URL/subjects/orders-value/versions")

echo "   Value Response: $RESPONSE_VAL"

echo ""
echo "📋 Registering Avro schema for subject 'orders-key'..."

SCHEMA_KEY=$(cat "$SCRIPT_DIR/test-data/order-key-schema.avsc" | python3 -c "
import sys, json
schema = json.load(sys.stdin)
payload = {'schema': json.dumps(schema), 'schemaType': 'AVRO'}
print(json.dumps(payload))
")

RESPONSE_KEY=$(curl -s -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "$SCHEMA_KEY" \
    "$SCHEMA_REGISTRY_URL/subjects/orders-key/versions")

echo "   Key Response: $RESPONSE_KEY"
echo "✅ Schemas registered!"

# Verify
echo ""
echo "🔍 Verifying schemas registration..."
curl -s "$SCHEMA_REGISTRY_URL/subjects/orders-value/versions/latest" | python3 -m json.tool
curl -s "$SCHEMA_REGISTRY_URL/subjects/orders-key/versions/latest" | python3 -m json.tool
echo ""

# -----------------------------------------------
# 4. Wait for Zeebe and deploy BPMN
# -----------------------------------------------
echo "⏳ Waiting for Zeebe to be ready..."
until curl -s "http://localhost:9600/ready" > /dev/null 2>&1; do
    echo "   Zeebe not ready, retrying in 5s..."
    sleep 5
done
echo "✅ Zeebe is ready!"

echo ""
echo "📄 Deploying test BPMN process..."
# Check if zbctl is available
if command -v zbctl &> /dev/null; then
    zbctl deploy "$SCRIPT_DIR/test-data/test-process.bpmn" \
        --address "$ZEEBE_ADDRESS" \
        --insecure
    echo "✅ BPMN process deployed!"
else
    echo "⚠️  zbctl not found. You can deploy the process manually:"
    echo "   zbctl deploy docker/test-data/test-process.bpmn --address $ZEEBE_ADDRESS --insecure"
    echo "   Or deploy via Operate UI at http://localhost:8083"
fi

# -----------------------------------------------
# 5. Print summary
# -----------------------------------------------
echo ""
echo "============================================"
echo " ✅ Setup Complete!"
echo "============================================"
echo ""
echo " Access Points:"
echo "   Zeebe Gateway:    localhost:26500"
echo "   Operate UI:       http://localhost:8083"
echo "   Schema Registry:  http://localhost:8081"
echo "   Kafka Broker:     localhost:9092"
echo "   Kafka UI:         http://localhost:8080"
echo "   Elasticsearch:    http://localhost:9200"
echo ""
echo " Next Steps:"
echo "   1. Start the worker:  mvn spring-boot:run"
echo "   2. Start a process instance (with zbctl):"
echo '      zbctl create instance kafka-avro-test-process \'
echo '        --variables '"'"'{"topic":"orders","schemaSubject":"orders-value","key":"order-001","value":{"orderId":"order-001","customerName":"John Doe","amount":150.99,"status":"APPROVED","priority":{"com.camunda.kafka.test.Priority":"HIGH"},"items":[{"productId":"SKU-100","quantity":2,"price":75.50}],"shippingAddress":{"com.camunda.kafka.test.Address":{"street":"123 Main St","city":"Munich","country":"DE"}}}}'"'"' \'
echo '        --address localhost:26500 --insecure'
echo ""
echo "   3. Check Kafka UI:  http://localhost:8080 → topic 'orders'"
echo ""
