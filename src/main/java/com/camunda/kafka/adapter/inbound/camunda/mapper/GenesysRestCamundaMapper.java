package com.camunda.kafka.adapter.inbound.camunda.mapper;

import com.camunda.kafka.adapter.inbound.camunda.variable.genesys.GenesysRestRequestResultVariables;
import com.camunda.kafka.adapter.inbound.camunda.variable.genesys.GenesysRestRequestVariables;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestRequest;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestResponse;
import org.springframework.stereotype.Component;

/**
 * Mapper that converts between Camunda DTOs and the domain DTOs used by the
 * use-case/service layer.
 */
@Component
public class GenesysRestCamundaMapper {

    /** Camunda → domain request */
    public GenesysRestRequest toDomain(GenesysRestRequestVariables vars) {
        return GenesysRestRequest.builder()
                .url(vars.getUrl())
                .method(vars.getMethod())
                .headers(vars.getHeaders())
                .body(vars.getBody())
                .queryParameters(vars.getQueryParameters())
                .connectionTimeoutInSeconds(vars.getConnectionTimeoutInSeconds())
                .readTimeoutInSeconds(vars.getReadTimeoutInSeconds())
                .build();
    }

    /** Domain response → Camunda result variables */
    public GenesysRestRequestResultVariables toResultVariables(GenesysRestResponse resp) {
        GenesysRestRequestResultVariables result = new GenesysRestRequestResultVariables();
        result.setStatus(resp.getStatus());
        result.setHeaders(resp.getHeaders());
        result.setBody(resp.getBody());
        return result;
    }
}
