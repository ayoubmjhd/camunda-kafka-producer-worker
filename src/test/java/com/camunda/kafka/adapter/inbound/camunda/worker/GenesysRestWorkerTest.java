package com.camunda.kafka.adapter.inbound.camunda.worker;

import com.camunda.kafka.adapter.inbound.camunda.mapper.GenesysRestCamundaMapper;
import com.camunda.kafka.adapter.inbound.camunda.variable.genesys.GenesysRestRequestResultVariables;
import com.camunda.kafka.adapter.inbound.camunda.variable.genesys.GenesysRestRequestVariables;
import com.camunda.kafka.domain.port.inbound.usecase.genesys.GenesysRestCallUseCase;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestRequest;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestResponse;
import com.camunda.kafka.exception.GenesysRestException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GenesysRestWorkerTest {

    @Mock
    private GenesysRestCallUseCase genesysRestCallUseCase;

    @Spy
    private GenesysRestCamundaMapper mapper = new GenesysRestCamundaMapper();

    @InjectMocks
    private GenesysRestWorker worker;

    private GenesysRestRequestVariables validRequest;

    @BeforeEach
    void setUp() {
        validRequest = new GenesysRestRequestVariables();
        validRequest.setUrl("/api/v2/users");
        validRequest.setMethod("GET");

        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        validRequest.setHeaders(headers);
    }

    @Test
    void executeRequest_Success() {
        // Arrange
        GenesysRestResponse domainResponse = GenesysRestResponse.builder()
                .status(200)
                .headers(new HashMap<>())
                .body("Success Body")
                .build();

        when(genesysRestCallUseCase.execute(any(GenesysRestRequest.class)))
                .thenReturn(domainResponse);

        // Act
        GenesysRestRequestResultVariables result = worker.executeRequest(validRequest);

        // Assert
        assertNotNull(result);
        assertEquals(200, result.getStatus());
        assertEquals("Success Body", result.getBody());

        verify(genesysRestCallUseCase, times(1)).execute(any(GenesysRestRequest.class));
        verify(mapper, times(1)).toDomain(validRequest);
        verify(mapper, times(1)).toResultVariables(domainResponse);
    }

    @Test
    void executeRequest_WithBody_Success() {
        // Arrange
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("name", "John Doe");
        requestBody.put("email", "john.doe@example.com");

        validRequest.setMethod("POST");
        validRequest.setBody(requestBody);

        Map<String, Object> responseBody = new HashMap<>();
        responseBody.put("id", "12345");

        GenesysRestResponse domainResponse = GenesysRestResponse.builder()
                .status(201)
                .headers(new HashMap<>())
                .body(responseBody)
                .build();

        when(genesysRestCallUseCase.execute(any(GenesysRestRequest.class)))
                .thenReturn(domainResponse);

        // Act
        GenesysRestRequestResultVariables result = worker.executeRequest(validRequest);

        // Assert
        assertNotNull(result);
        assertEquals(201, result.getStatus());
        assertEquals(responseBody, result.getBody());

        // Verify mapper was called with correct body
        verify(mapper).toDomain(argThat(vars ->
                vars.getBody() != null && vars.getBody().equals(requestBody)));
    }

    @Test
    void executeRequest_MissingUrl_ThrowsException() {
        // Arrange
        validRequest.setUrl(null);

        // Act & Assert
        GenesysRestException exception = assertThrows(GenesysRestException.class, () -> {
            worker.executeRequest(validRequest);
        });

        assertEquals("'url' process variable is required", exception.getMessage());
        verifyNoInteractions(genesysRestCallUseCase);
    }

    @Test
    void executeRequest_MissingMethod_ThrowsException() {
        // Arrange
        validRequest.setMethod(null);

        // Act & Assert
        GenesysRestException exception = assertThrows(GenesysRestException.class, () -> {
            worker.executeRequest(validRequest);
        });

        assertEquals("'method' process variable is required", exception.getMessage());
        verifyNoInteractions(genesysRestCallUseCase);
    }

    @Test
    void executeRequest_InvalidMethod_ThrowsException() {
        // Arrange
        validRequest.setMethod("INVALID");

        // Act & Assert
        GenesysRestException exception = assertThrows(GenesysRestException.class, () -> {
            worker.executeRequest(validRequest);
        });

        assertTrue(exception.getMessage().contains("Invalid HTTP method"));
        verifyNoInteractions(genesysRestCallUseCase);
    }

    @Test
    void executeRequest_ServiceThrowsException_RethrowsException() {
        // Arrange
        GenesysRestException expectedException = new GenesysRestException("API Failed");

        when(genesysRestCallUseCase.execute(any(GenesysRestRequest.class)))
                .thenThrow(expectedException);

        // Act & Assert
        GenesysRestException exception = assertThrows(GenesysRestException.class, () -> {
            worker.executeRequest(validRequest);
        });

        assertEquals("API Failed", exception.getMessage());
    }

    @Test
    void executeRequest_WithQueryParams_MappedCorrectly() {
        // Arrange
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("page", 1);
        queryParams.put("size", 25);
        validRequest.setQueryParameters(queryParams);

        GenesysRestResponse domainResponse = GenesysRestResponse.builder()
                .status(200)
                .headers(new HashMap<>())
                .body(null)
                .build();

        when(genesysRestCallUseCase.execute(any(GenesysRestRequest.class)))
                .thenReturn(domainResponse);

        // Act
        worker.executeRequest(validRequest);

        // Assert – verify mapper passes queryParameters through
        verify(mapper).toDomain(argThat(vars ->
                vars.getQueryParameters() != null
                        && vars.getQueryParameters().get("page").equals(1)
                        && vars.getQueryParameters().get("size").equals(25)));
    }
}
