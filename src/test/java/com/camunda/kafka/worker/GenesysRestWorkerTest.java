package com.camunda.kafka.worker;

import com.camunda.kafka.exception.GenesysRestException;
import com.camunda.kafka.model.GenesysRestRequestVariables;
import com.camunda.kafka.model.GenesysRestRequestResultVariables;
import com.camunda.kafka.service.GenesysRestService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GenesysRestWorkerTest {

    @Mock
    private GenesysRestService genesysRestService;

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
        GenesysRestRequestResultVariables mockResponse = new GenesysRestRequestResultVariables(200, new HashMap<>(), "Success Body");
        
        when(genesysRestService.execute(
                eq(validRequest.getUrl()),
                eq(validRequest.getMethod()),
                eq(validRequest.getHeaders()),
                eq(validRequest.getBody()),
                eq(validRequest.getQueryParameters()),
                eq(validRequest.getConnectionTimeoutInSeconds()),
                eq(validRequest.getReadTimeoutInSeconds())
        )).thenReturn(mockResponse);

        // Act
        GenesysRestRequestResultVariables result = worker.executeRequest(validRequest);

        // Assert
        assertNotNull(result);
        assertEquals(200, result.getStatus());
        assertEquals("Success Body", result.getBody());
        
        verify(genesysRestService, times(1)).execute(
                any(), any(), any(), any(), any(), any(), any()
        );
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
        
        GenesysRestRequestResultVariables mockResponse = new GenesysRestRequestResultVariables(201, new HashMap<>(), responseBody);
        
        when(genesysRestService.execute(
                eq(validRequest.getUrl()),
                eq(validRequest.getMethod()),
                eq(validRequest.getHeaders()),
                eq(requestBody),
                eq(validRequest.getQueryParameters()),
                eq(validRequest.getConnectionTimeoutInSeconds()),
                eq(validRequest.getReadTimeoutInSeconds())
        )).thenReturn(mockResponse);

        // Act
        GenesysRestRequestResultVariables result = worker.executeRequest(validRequest);

        // Assert
        assertNotNull(result);
        assertEquals(201, result.getStatus());
        assertEquals(responseBody, result.getBody());
        
        verify(genesysRestService, times(1)).execute(
                any(), any(), any(), eq(requestBody), any(), any(), any()
        );
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
        verifyNoInteractions(genesysRestService);
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
        verifyNoInteractions(genesysRestService);
    }

    @Test
    void executeRequest_ServiceThrowsException_RethrowsException() {
        // Arrange
        GenesysRestException expectedException = new GenesysRestException("API Failed");
        
        when(genesysRestService.execute(any(), any(), any(), any(), any(), any(), any()))
                .thenThrow(expectedException);

        // Act & Assert
        GenesysRestException exception = assertThrows(GenesysRestException.class, () -> {
            worker.executeRequest(validRequest);
        });
        
        assertEquals("API Failed", exception.getMessage());
    }
}
