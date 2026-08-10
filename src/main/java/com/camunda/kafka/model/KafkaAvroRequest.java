package com.camunda.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Generic request POJO for the Kafka Avro Producer job worker.
 * Mapped via {@code @VariablesAsType} from Camunda process variables.
 *
 * <p>Process variables expected:
 * <ul>
 *   <li>{@code topic} - Kafka topic name</li>
 *   <li>{@code schemaSubject} - Schema Registry subject (e.g. "orders-value")</li>
 *   <li>{@code key} - Kafka message key</li>
 *   <li>{@code value} - Dynamic payload as a Map, converted to Avro GenericRecord</li>
 * </ul>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaAvroRequest {

    private String topic;
    private String schemaSubject;
    private Map<String, Object> key;
    private Map<String, Object> value;
}
