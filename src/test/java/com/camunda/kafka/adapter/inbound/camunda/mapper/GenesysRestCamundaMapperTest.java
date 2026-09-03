package com.camunda.kafka.adapter.inbound.camunda.mapper;

import com.camunda.kafka.adapter.inbound.camunda.variable.genesys.GenesysRestRequestResultVariables;
import com.camunda.kafka.adapter.inbound.camunda.variable.genesys.GenesysRestRequestVariables;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestRequest;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestResponse;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GenesysRestCamundaMapperTest {

    private final GenesysRestCamundaMapper mapper = new GenesysRestCamundaMapper();

    @Test
    void toDomain_shouldMapVariablesToDomainRequest() {
        // Arrange
        GenesysRestRequestVariables vars = new GenesysRestRequestVariables();
        vars.setUrl("/api/test");
        vars.setMethod("POST");
        vars.setHeaders(Map.of("Authorization", "Bearer token"));
        vars.setQueryParameters(Map.of("id", "123"));
        vars.setBody("requestBody");

        // Act
        GenesysRestRequest request = mapper.toDomain(vars);

        // Assert
        assertEquals("/api/test", request.getUrl());
        assertEquals("POST", request.getMethod());
        assertEquals(Map.of("Authorization", "Bearer token"), request.getHeaders());
        assertEquals(Map.of("id", "123"), request.getQueryParameters());
        assertEquals("requestBody", request.getBody());
    }

    @Test
    void toResultVariables_shouldMapDomainResponseToVariables() {
        // Arrange
        GenesysRestResponse response = GenesysRestResponse.builder()
                .status(200)
                .headers(Map.of("Content-Type", "application/json"))
                .body("responseBody")
                .build();

        // Act
        GenesysRestRequestResultVariables vars = mapper.toResultVariables(response);

        // Assert
        assertEquals(200, vars.getStatus());
        assertEquals(Map.of("Content-Type", "application/json"), vars.getHeaders());
        assertEquals("responseBody", vars.getBody());
    }
}
