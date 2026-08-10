package com.camunda.kafka.exception;

/**
 * Custom exception for Kafka producer errors such as schema validation failures,
 * invalid enum values, or serialization issues.
 */
public class KafkaProducerException extends RuntimeException {

    public KafkaProducerException(String message) {
        super(message);
    }

    public KafkaProducerException(String message, Throwable cause) {
        super(message, cause);
    }
}
