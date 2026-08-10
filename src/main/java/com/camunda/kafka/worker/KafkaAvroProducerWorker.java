package com.camunda.kafka.worker;

import com.camunda.kafka.converter.AvroRecordBuilder;
import com.camunda.kafka.exception.KafkaProducerException;
import com.camunda.kafka.model.KafkaAvroRequest;
import com.camunda.kafka.model.KafkaProducerResponse;
import com.camunda.kafka.service.SchemaRegistryService;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.VariablesAsType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Generic Camunda 8.6 Job Worker that produces Kafka messages with Avro serialization.
 *
 * <p>Usage in BPMN: Create a Service Task with type {@code kafka-avro-producer}
 * and set the following process variables:
 * <ul>
 *   <li>{@code topic} - Kafka topic name</li>
 *   <li>{@code schemaSubject} - Schema Registry subject (e.g., "orders-value")</li>
 *   <li>{@code key} - Kafka message key</li>
 *   <li>{@code value} - Message payload as a JSON/Map object</li>
 * </ul>
 *
 * <p>The worker handles all Avro types including ENUM, which the official
 * Camunda Kafka connector does not support.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaAvroProducerWorker {

    private static final long SEND_TIMEOUT_SECONDS = 45;

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final SchemaRegistryService schemaRegistryService;
    private final AvroRecordBuilder avroRecordBuilder;

    /**
     * Job worker handler. Receives process variables via {@code @VariablesAsType},
     * builds an Avro GenericRecord, and publishes to Kafka.
     *
     * @param request the Kafka publish request (topic, schemaSubject, key, value)
     * @return response with metadata (topic, timestamp, offset, partition)
     */
    @SuppressWarnings("unchecked")
    @JobWorker(type = "kafka-avro-producer")
    public KafkaProducerResponse publishToKafka(@VariablesAsType KafkaAvroRequest request) {
        log.info("Received Kafka publish request: topic={}, subject={}, key={}",
                request.getTopic(), request.getSchemaSubject(), request.getKey());

        validateRequest(request);

        try {
            // 1. Fetch schema from Schema Registry by subject name
            Schema schema = schemaRegistryService.getSchema(request.getSchemaSubject());
            log.debug("Schema fetched: {}", schema.getFullName());

            // 2. Build GenericRecord from the value map (handles ENUMs, nested records, etc.)
            GenericRecord record = avroRecordBuilder.buildRecord(schema, request.getValue());
            log.debug("GenericRecord built successfully with {} fields", record.getSchema().getFields().size());

            // 3. Build GenericRecord for the key from the key map
            String keySubject = request.getTopic() + "-key";
            log.debug("Fetching key schema for subject: {}", keySubject);
            Schema keySchema = schemaRegistryService.getSchema(keySubject);
            GenericRecord keyRecord = avroRecordBuilder.buildRecord(keySchema, request.getKey());
            log.debug("Key GenericRecord built successfully with {} fields", keyRecord.getSchema().getFields().size());

            // 4. Send to Kafka (synchronous wait for acknowledgment)
            CompletableFuture<SendResult<Object, Object>> future =
                    kafkaTemplate.send(request.getTopic(), keyRecord, record);

            SendResult<Object, Object> sendResult =
                    future.get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // 5. Build response with metadata
            RecordMetadata metadata = sendResult.getRecordMetadata();
            KafkaProducerResponse response = new KafkaProducerResponse(
                    metadata.topic(),
                    metadata.timestamp(),
                    metadata.offset(),
                    metadata.partition()
            );

            log.info("Message published successfully: topic={}, partition={}, offset={}",
                    metadata.topic(), metadata.partition(), metadata.offset());

            return response;

        } catch (KafkaProducerException e) {
            log.error("Schema/conversion error: {}", e.getMessage(), e);
            throw e;
        } catch (ExecutionException e) {
            log.error("Kafka send failed: {}", e.getCause().getMessage(), e);
            throw new KafkaProducerException(
                    "Failed to send message to Kafka: " + e.getCause().getMessage(), e.getCause());
        } catch (TimeoutException e) {
            log.error("Kafka send timed out after {}s", SEND_TIMEOUT_SECONDS);
            throw new KafkaProducerException(
                    "Kafka send timed out after " + SEND_TIMEOUT_SECONDS + " seconds", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaProducerException("Kafka send was interrupted", e);
        }
    }

    private void validateRequest(KafkaAvroRequest request) {
        if (request.getTopic() == null || request.getTopic().isBlank()) {
            throw new KafkaProducerException("'topic' process variable is required");
        }
        if (request.getSchemaSubject() == null || request.getSchemaSubject().isBlank()) {
            throw new KafkaProducerException("'schemaSubject' process variable is required");
        }
        if (request.getKey() == null || request.getKey().isEmpty()) {
            throw new KafkaProducerException("'key' process variable is required and must not be empty");
        }
        if (request.getValue() == null || request.getValue().isEmpty()) {
            throw new KafkaProducerException("'value' process variable is required and must not be empty");
        }
    }
}
