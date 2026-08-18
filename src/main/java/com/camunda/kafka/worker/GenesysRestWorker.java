package com.camunda.kafka.worker;

import com.camunda.kafka.exception.GenesysRestException;
import com.camunda.kafka.model.GenesysRestRequestVariables;
import com.camunda.kafka.model.GenesysRestRequestResultVariables;
import com.camunda.kafka.service.GenesysRestService;
import io.camunda.zeebe.spring.client.annotation.JobWorker;
import io.camunda.zeebe.spring.client.annotation.VariablesAsType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Camunda 8 Job Worker that acts as a <strong>drop-in replacement</strong>
 * for the REST Connector, specifically for Genesys Cloud API calls.
 *
 * <p>This worker solves the HTTP 408 timeout issue by:
 * <ul>
 *   <li>Using generous HTTP timeouts (default 120s read vs the connector's ~20s)</li>
 *   <li>Delegating exponential backoff to Zeebe by throwing exceptions on 408, 429, and 5xx errors</li>
 *   <li>Managing OAuth2 tokens centrally (no more tokens in BPMN headers)</li>
 *   <li>Auto-refreshing expired tokens and handling 401 transparently</li>
 * </ul>
 *
 * <p><strong>Usage in BPMN:</strong> Change the service task type from
 * {@code io.camunda:http-json:1} to {@code genesys-rest} and remove
 * any authentication headers — they're handled automatically.
 *
 * <p><strong>URL handling:</strong> If the {@code url} starts with "/",
 * the Genesys base URL from config is prepended automatically.
 * For example, set url to {@code /api/v2/conversations/emails/agentless}
 * and the worker resolves it to {@code https://api.mypurecloud.de/api/v2/conversations/emails/agentless}.
 *
 * <p><strong>Output:</strong> Returns a {@code result} variable with
 * {@code status}, {@code headers}, and {@code body} — same structure
 * as the REST Connector, so existing output mappings still work.
 *
 * <p>Process variables (same names as REST Connector):
 * <ul>
 *   <li>{@code url} - API path or full URL (required)</li>
 *   <li>{@code method} - HTTP method: GET, POST, PUT, DELETE, PATCH (required)</li>
 *   <li>{@code headers} - Additional HTTP headers as Map (optional)</li>
 *   <li>{@code body} - Request body (optional)</li>
 *   <li>{@code queryParameters} - Query parameters as Map (optional)</li>
 *   <li>{@code connectionTimeoutInSeconds} - Connect timeout override (optional)</li>
 *   <li>{@code readTimeoutInSeconds} - Read timeout override (optional)</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class GenesysRestWorker {

    private final GenesysRestService genesysRestService;

    /**
     * Job worker handler. Receives process variables via {@code @VariablesAsType},
     * validates them, and executes the HTTP request against Genesys Cloud.
     *
     * @param request the REST request (mapped from process variables)
     * @return response with status, headers, and body
     */
    @JobWorker(type = "genesys-rest")
    public GenesysRestRequestResultVariables executeRequest(@VariablesAsType GenesysRestRequestVariables request) {
        log.info("Received Genesys REST request: method={}, url={}",
                request.getMethod(), request.getUrl());

        validateRequest(request);

        try {
            GenesysRestRequestResultVariables response = genesysRestService.execute(
                    request.getUrl(),
                    request.getMethod(),
                    request.getHeaders(),
                    request.getBody(),
                    request.getQueryParameters(),
                    request.getConnectionTimeoutInSeconds(),
                    request.getReadTimeoutInSeconds()
            );

            log.info("Genesys REST call completed: method={}, url={}, status={}",
                    request.getMethod(), request.getUrl(), response.getStatus());

            return response;

        } catch (GenesysRestException e) {
            log.error("Genesys REST call failed: method={}, url={}, error={}",
                    request.getMethod(), request.getUrl(), e.getMessage(), e);
            throw e;
        }
    }

    private void validateRequest(GenesysRestRequestVariables request) {
        if (request.getUrl() == null || request.getUrl().isBlank()) {
            throw new GenesysRestException("'url' process variable is required");
        }
        if (request.getMethod() == null || request.getMethod().isBlank()) {
            throw new GenesysRestException("'method' process variable is required");
        }

        String method = request.getMethod().toUpperCase();
        if (!method.matches("GET|POST|PUT|DELETE|PATCH")) {
            throw new GenesysRestException("Invalid HTTP method: " + request.getMethod()
                    + ". Supported: GET, POST, PUT, DELETE, PATCH");
        }
    }
}
