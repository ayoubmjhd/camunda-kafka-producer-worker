package com.camunda.kafka.adapter.inbound.camunda.worker;

import com.camunda.kafka.adapter.inbound.camunda.mapper.GenesysRestCamundaMapper;
import com.camunda.kafka.adapter.inbound.camunda.variable.genesys.GenesysRestRequestResultVariables;
import com.camunda.kafka.adapter.inbound.camunda.variable.genesys.GenesysRestRequestVariables;
import com.camunda.kafka.domain.port.inbound.usecase.genesys.GenesysRestCallUseCase;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestRequest;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestResponse;
import com.camunda.kafka.exception.GenesysRestException;
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
 *
 * <p><strong>Output:</strong> Returns a {@code result} variable with
 * {@code status}, {@code headers}, and {@code body} — same structure
 * as the REST Connector, so existing output mappings still work.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class GenesysRestWorker {

    private final GenesysRestCallUseCase genesysRestCallUseCase;
    private final GenesysRestCamundaMapper mapper;

    /**
     * Job worker handler. Receives process variables via {@code @VariablesAsType},
     * validates them, maps to domain, executes via use-case, and maps the response back.
     *
     * @param requestVariables the REST request (mapped from process variables)
     * @return response with status, headers, and body
     */
    @JobWorker(type = "genesys-rest")
    public GenesysRestRequestResultVariables executeRequest(
            @VariablesAsType GenesysRestRequestVariables requestVariables) {

        log.info("Genesys REST call – method='{}' url='{}'",
                requestVariables.getMethod(), requestVariables.getUrl());

        validateRequest(requestVariables);

        try {
            // 1️⃣ Camunda → domain request
            GenesysRestRequest domainRequest = mapper.toDomain(requestVariables);

            // 2️⃣ Execute the use-case (which delegates to the outbound connector)
            GenesysRestResponse domainResponse = genesysRestCallUseCase.execute(domainRequest);

            log.info("Genesys REST call completed: method='{}', url='{}', status={}",
                    requestVariables.getMethod(), requestVariables.getUrl(),
                    domainResponse.getStatus());

            // 3️⃣ Domain response → Camunda result variables
            return mapper.toResultVariables(domainResponse);

        } catch (GenesysRestException e) {
            log.error("Genesys REST call failed: method={}, url={}, error={}",
                    requestVariables.getMethod(), requestVariables.getUrl(),
                    e.getMessage(), e);
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
