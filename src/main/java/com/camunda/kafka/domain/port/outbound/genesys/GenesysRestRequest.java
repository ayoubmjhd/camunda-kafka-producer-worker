package com.camunda.kafka.domain.port.outbound.genesys;

import lombok.Builder;
import lombok.Value;

import java.util.Map;

/**
 * Immutable DTO that represents a Genesys REST request.
 */
@Value
@Builder
public class GenesysRestRequest {
    String url;
    String method;
    Map<String, String> headers;
    Object body;
    Map<String, Object> queryParameters;
    Integer connectionTimeoutInSeconds;
    Integer readTimeoutInSeconds;
}
