package com.camunda.kafka.service;

import com.camunda.kafka.config.GenesysProperties;
import com.camunda.kafka.exception.GenesysRestException;
import com.camunda.kafka.model.GenesysRestRequestResultVariables;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.*;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GenesysRestServiceTest {

    @Mock
    private RestTemplate defaultRestTemplate;
    @Mock
    private RestTemplateBuilder restTemplateBuilder;
    @Mock
    private GenesysProperties config;
    @Mock
    private GenesysAuthService authService;

    private ObjectMapper objectMapper;
    private GenesysRestService service;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        service = new GenesysRestService(
                defaultRestTemplate,
                restTemplateBuilder,
                config,
                authService,
                objectMapper
        );
    }

    @Test
    void execute_ResolvesUrlAndInjectsToken() {
        // Arrange
        when(config.getApiBaseUrl()).thenReturn("https://api.mypurecloud.de");
        when(authService.getAccessToken()).thenReturn("mock-token");

        ResponseEntity<String> mockEntity = new ResponseEntity<>("{\"success\":true}", HttpStatus.OK);
        when(defaultRestTemplate.exchange(
                eq("https://api.mypurecloud.de/api/v2/users"),
                eq(HttpMethod.GET),
                any(HttpEntity.class),
                eq(String.class)
        )).thenReturn(mockEntity);

        // Act
        GenesysRestRequestResultVariables response = service.execute(
                "/api/v2/users",
                "GET",
                null,
                null,
                null,
                null,
                null
        );

        // Assert
        assertEquals(200, response.getStatus());
        
        ArgumentCaptor<HttpEntity<Object>> entityCaptor = ArgumentCaptor.forClass(HttpEntity.class);
        verify(defaultRestTemplate).exchange(anyString(), any(HttpMethod.class), entityCaptor.capture(), eq(String.class));
        
        HttpEntity<Object> capturedEntity = entityCaptor.getValue();
        assertEquals("Bearer mock-token", capturedEntity.getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
        assertEquals(MediaType.APPLICATION_JSON, capturedEntity.getHeaders().getContentType());
    }

    @Test
    void execute_FormUrlEncodedBody_ConvertsToMultiValueMap() {
        // Arrange
        when(config.getApiBaseUrl()).thenReturn("https://api.mypurecloud.de");
        when(authService.getAccessToken()).thenReturn("mock-token");

        Map<String, String> customHeaders = new HashMap<>();
        customHeaders.put(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE);

        Map<String, Object> bodyMap = new HashMap<>();
        bodyMap.put("grant_type", "client_credentials");

        ResponseEntity<String> mockEntity = new ResponseEntity<>("OK", HttpStatus.OK);
        when(defaultRestTemplate.exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), eq(String.class)))
                .thenReturn(mockEntity);

        // Act
        service.execute("/oauth/token", "POST", customHeaders, bodyMap, null, null, null);

        // Assert
        ArgumentCaptor<HttpEntity<Object>> entityCaptor = ArgumentCaptor.forClass(HttpEntity.class);
        verify(defaultRestTemplate).exchange(anyString(), any(HttpMethod.class), entityCaptor.capture(), eq(String.class));
        
        HttpEntity<Object> capturedEntity = entityCaptor.getValue();
        assertTrue(capturedEntity.getBody() instanceof MultiValueMap);
        
        @SuppressWarnings("unchecked")
        MultiValueMap<String, Object> formBody = (MultiValueMap<String, Object>) capturedEntity.getBody();
        assertEquals("client_credentials", formBody.getFirst("grant_type"));
    }

    @Test
    void execute_401Unauthorized_RefreshesTokenAndRetries() {
        // Arrange
        when(config.getApiBaseUrl()).thenReturn("https://api.mypurecloud.de");
        when(authService.getAccessToken()).thenReturn("old-token").thenReturn("new-token");

        // First call throws 401
        when(defaultRestTemplate.exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), eq(String.class)))
                .thenThrow(HttpClientErrorException.create(HttpStatus.UNAUTHORIZED, "Unauthorized", HttpHeaders.EMPTY, null, null))
                // Second call succeeds
                .thenReturn(new ResponseEntity<>("OK", HttpStatus.OK));

        // Act
        GenesysRestRequestResultVariables response = service.execute("/api/test", "GET", null, null, null, null, null);

        // Assert
        assertEquals(200, response.getStatus());
        verify(authService, times(1)).invalidateToken();
        verify(defaultRestTemplate, times(2)).exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), eq(String.class));
    }
    
    @Test
    void execute_408Timeout_ThrowsExceptionForZeebe() {
        // Arrange
        when(config.getApiBaseUrl()).thenReturn("https://api.mypurecloud.de");
        
        when(defaultRestTemplate.exchange(anyString(), any(HttpMethod.class), any(HttpEntity.class), eq(String.class)))
                .thenThrow(HttpClientErrorException.create(HttpStatus.REQUEST_TIMEOUT, "Timeout", HttpHeaders.EMPTY, null, null));

        // Act & Assert
        GenesysRestException exception = assertThrows(GenesysRestException.class, () -> {
            service.execute("/api/test", "GET", null, null, null, null, null);
        });
        
        assertTrue(exception.getMessage().contains("408"));
    }
}
