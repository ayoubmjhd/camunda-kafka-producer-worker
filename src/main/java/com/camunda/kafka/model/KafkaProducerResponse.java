package com.camunda.kafka.model;

/**
 * Response record returned by the job worker after successful Kafka publish.
 * Becomes a process variable accessible to downstream tasks.
 */
public record KafkaProducerResponse(
        String topic,
        long timestamp,
        long offset,
        int partition
) {
}
