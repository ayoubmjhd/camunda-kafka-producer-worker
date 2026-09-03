package com.camunda.kafka.infrastructure.security;

import com.camunda.kafka.application.port.outbound.GenesysTokenProvider;
import com.camunda.kafka.config.GenesysProperties;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

class GenesysRestClientConfigTest {

    @Test
    void shouldCreateAuthRestClient() {
        // Arrange
        GenesysProperties properties = new GenesysProperties();
        properties.setConnectTimeoutSeconds(5);
        properties.setReadTimeoutSeconds(10);
        
        GenesysRestClientConfig config = new GenesysRestClientConfig(properties);
        
        // Act
        RestClient authClient = config.genesysAuthRestClient();
        
        // Assert
        assertNotNull(authClient, "Auth RestClient should not be null");
    }

    @Test
    void shouldCreateApiRestClient() {
        // Arrange
        GenesysProperties properties = new GenesysProperties();
        properties.setConnectTimeoutSeconds(5);
        properties.setReadTimeoutSeconds(10);
        
        GenesysTokenProvider tokenProvider = mock(GenesysTokenProvider.class);
        GenesysRestClientConfig config = new GenesysRestClientConfig(properties);
        
        // Act
        RestClient apiClient = config.genesysApiRestClient(tokenProvider);
        
        // Assert
        assertNotNull(apiClient, "API RestClient should not be null");
    }
}
