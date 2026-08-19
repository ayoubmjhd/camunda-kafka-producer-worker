package com.camunda.kafka.domain.port.outbound.genesys;

import lombok.Builder;
import lombok.Value;

import java.util.Map;

/**
 * Immutable DTO that represents a Genesys REST response.
 */
@Value
@Builder
public class GenesysRestResponse {
    int status;
    Map<String, String> headers;
    Object body;
}
