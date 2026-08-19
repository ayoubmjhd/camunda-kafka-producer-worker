package com.camunda.kafka.adapter.inbound.camunda.variable.genesys;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Camunda input variables for a generic Genesys REST call.
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

    /** Full URL (or relative path if a base URL is configured). */
    @NotBlank
    private String url;

    /** HTTP method – GET, POST, PUT, DELETE, PATCH */
    @NotBlank
    private String method;

    /** Optional additional HTTP headers. */
    private Map<String, String> headers;

    /** Request body – can be a POJO or a raw JSON string. */
    private Object body;

    /** Optional query-string parameters. */
    private Map<String, Object> queryParameters;

    /** Connection timeout in seconds (optional). */
    private Integer connectionTimeoutInSeconds;

    /** Read timeout in seconds (optional). */
    private Integer readTimeoutInSeconds;
}
