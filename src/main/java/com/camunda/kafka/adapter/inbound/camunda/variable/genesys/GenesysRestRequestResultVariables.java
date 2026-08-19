package com.camunda.kafka.adapter.inbound.camunda.variable.genesys;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Camunda output variables returned after the Genesys REST call.
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

    /** HTTP status code returned by Genesys. */
    private int status;

    /** Selected response headers (flattened to a simple map). */
    private Map<String, String> headers;

    /** Response body – can be a String, Map, POJO, … */
    private Object body;
}
