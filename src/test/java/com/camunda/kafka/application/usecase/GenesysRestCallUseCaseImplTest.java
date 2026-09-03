package com.camunda.kafka.application.usecase;

import com.camunda.kafka.application.port.outbound.GenesysRestOutboundPort;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestRequest;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GenesysRestCallUseCaseImplTest {

    @Mock
    private GenesysRestOutboundPort genesysRestOutboundPort;

    @InjectMocks
    private GenesysRestCallUseCaseImpl useCase;

    @Test
    void execute_shouldForwardRequestToOutboundPort() {
        // Arrange
        GenesysRestRequest request = GenesysRestRequest.builder()
                .url("/test")
                .method("GET")
                .build();
                
        GenesysRestResponse expectedResponse = GenesysRestResponse.builder()
                .status(200)
                .build();
                
        when(genesysRestOutboundPort.call(request)).thenReturn(expectedResponse);

        // Act
        GenesysRestResponse actualResponse = useCase.execute(request);

        // Assert
        assertSame(expectedResponse, actualResponse);
        verify(genesysRestOutboundPort).call(request);
    }
}
