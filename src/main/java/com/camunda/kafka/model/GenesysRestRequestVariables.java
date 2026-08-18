package com.camunda.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Generic Genesys REST request POJO for the Genesys REST Worker.
 * Mapped via {@code @VariablesAsType} from Camunda process variables.
 *
 * <p>Uses the <strong>same variable names</strong> as the Camunda REST Connector,
 * making it a drop-in replacement. Just change the service task type from
 * {@code io.camunda:http-json:1} to {@code genesys-rest}.
 *
 * <p>Process variables expected:
 * <ul>
 *   <li>{@code url} - Genesys API path (e.g., "/api/v2/conversations/emails/agentless")
 *       or full URL. If it starts with "/", the Genesys base URL is prepended automatically. (required)</li>
 *   <li>{@code method} - HTTP method: GET, POST, PUT, DELETE, PATCH (required)</li>
 *   <li>{@code headers} - Additional HTTP headers as a Map (optional, Bearer token is auto-injected)</li>
 *   <li>{@code body} - Request body as a Map/Object (optional, used with POST/PUT/PATCH)</li>
 *   <li>{@code queryParameters} - Query parameters as a Map (optional)</li>
 *   <li>{@code connectionTimeoutInSeconds} - Connect timeout override (optional, defaults to config)</li>
 *   <li>{@code readTimeoutInSeconds} - Read timeout override (optional, defaults to config)</li>
 * </ul>
 *
 * <p><strong>Note:</strong> OAuth2 authentication is handled automatically.
 * You do NOT need to set any authentication headers in the BPMN.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GenesysRestRequestVariables {

    private String url;
    private String method;
    private Map<String, String> headers;
    private Object body;
    private Map<String, Object> queryParameters;
    private Integer connectionTimeoutInSeconds;
    private Integer readTimeoutInSeconds;
}
