package com.camunda.kafka.exception;

/**
 * Custom exception for Genesys REST Worker errors such as HTTP failures,
 * authentication errors, or timeout issues.
 */
public class GenesysRestException extends RuntimeException {

    public GenesysRestException(String message) {
        super(message);
    }

    public GenesysRestException(String message, Throwable cause) {
        super(message, cause);
    }
}
