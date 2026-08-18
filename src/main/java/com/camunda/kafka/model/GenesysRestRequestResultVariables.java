package com.camunda.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Generic Genesys REST response returned by the worker.
 * Mirrors the Camunda REST Connector output structure so existing
 * output mappings in BPMN continue to work (e.g., {@code result.body}).
 *
 * <p>Fields:
 * <ul>
 *   <li>{@code status} - HTTP status code (e.g., 200)</li>
 *   <li>{@code headers} - Response headers as a Map</li>
 *   <li>{@code body} - Response body (parsed as Map/List if JSON, raw String otherwise)</li>
 * </ul>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GenesysRestRequestResultVariables {

    private int status;
    private Map<String, String> headers;
    private Object body;
}
