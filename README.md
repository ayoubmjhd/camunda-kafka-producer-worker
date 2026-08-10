# Camunda Kafka Avro Producer Worker

A generic Camunda 8.6 Job Worker that produces Kafka messages with Avro serialization, including full **ENUM type support** that the built-in Camunda Kafka connector lacks.

## Why?

The official Camunda 8.6 Kafka Producer Connector's `GenericRecordConverter` does **not handle Avro ENUM types**, causing `AvroTypeException` when process variables contain enum values. This worker fixes that by properly wrapping string values as `GenericData.EnumSymbol`.

## Quick Start

### 1. Start the Docker Environment

```bash
cd docker
docker compose up -d
```

This starts:
- **Camunda 8.6**: Zeebe (`:26500`), Operate (`:8083`), Elasticsearch
- **Kafka Stack**: Kafka (`:9092`), Schema Registry (`:8081`), Kafka UI (`:8080`)

### 2. Setup Test Data (Topics + Schemas + BPMN)

```bash
chmod +x docker/setup.sh
./docker/setup.sh
```

### 3. Build and Run the Worker

```bash
mvn clean package -DskipTests
mvn spring-boot:run
```

### 4. Create a Test Process Instance

```bash
zbctl create instance kafka-avro-test-process \
  --variables '{"topic":"orders","schemaSubject":"orders-value","key":"order-001","value":{"orderId":"order-001","customerName":"John Doe","amount":150.99,"status":"APPROVED","priority":"HIGH","items":[{"productId":"SKU-100","quantity":2,"price":75.50}],"shippingAddress":{"street":"123 Main St","city":"Munich","country":"DE"}}}' \
  --address localhost:26500 --insecure
```

### 5. Verify

- **Kafka UI**: [http://localhost:8080](http://localhost:8080) → check `orders` topic
- **Operate**: [http://localhost:8083](http://localhost:8083) → check process instance

## Usage in BPMN

1. Create a **Service Task** with type: `kafka-avro-producer`
2. Set these **process variables** before the task:

| Variable | Type | Description | Example |
|---|---|---|---|
| `topic` | String | Kafka topic name | `"orders"` |
| `schemaSubject` | String | Schema Registry subject | `"orders-value"` |
| `key` | String | Kafka message key | `"order-123"` |
| `value` | Map/JSON | Message payload | `{"orderId": "123", "status": "APPROVED"}` |

The worker returns a `KafkaProducerResponse` with: `topic`, `timestamp`, `offset`, `partition`.

## Supported Avro Types

| Avro Type | Handling |
|---|---|
| `STRING` | Direct |
| `INT`, `LONG`, `FLOAT`, `DOUBLE` | Numeric coercion |
| `BOOLEAN` | Direct |
| **`ENUM`** ⭐ | `GenericData.EnumSymbol` with validation |
| `RECORD` | Recursive conversion |
| `ARRAY` | Element-wise conversion |
| `MAP` | Value-wise conversion |
| `UNION` | Type resolution (including `["null", "EnumType"]`) |
| `BYTES`, `FIXED` | Byte array conversion |

## Configuration

Environment variables (with defaults for local Docker):

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker(s) |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent Schema Registry |
| `ZEEBE_GATEWAY` | `localhost:26500` | Camunda Zeebe gateway |

## Running Tests

```bash
mvn clean test
```

## Project Structure

```
src/main/java/com/camunda/kafka/
├── CamundaKafkaApplication.java        # Entry point
├── config/KafkaProducerConfig.java     # Kafka + Avro producer config
├── converter/AvroRecordBuilder.java    # Map → GenericRecord (ENUM fix)
├── exception/KafkaProducerException.java
├── model/
│   ├── KafkaAvroRequest.java           # @VariablesAsType POJO
│   └── KafkaProducerResponse.java      # Result record
├── service/SchemaRegistryService.java  # Schema fetch by subject
└── worker/KafkaAvroProducerWorker.java # The job worker
```
