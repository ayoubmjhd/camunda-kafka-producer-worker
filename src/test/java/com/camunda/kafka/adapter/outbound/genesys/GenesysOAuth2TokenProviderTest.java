package com.camunda.kafka.adapter.outbound.genesys;

import com.camunda.kafka.config.GenesysProperties;
import com.camunda.kafka.exception.GenesysRestException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GenesysOAuth2TokenProviderTest {

    @Mock
    private RestClient restClient;

    @Mock
    private RestClient.RequestBodyUriSpec requestBodyUriSpec;

    @Mock
    private RestClient.RequestBodySpec requestBodySpec;

    @Mock
    private RestClient.ResponseSpec responseSpec;

    private GenesysProperties properties;
    private GenesysOAuth2TokenProvider tokenProvider;

    @BeforeEach
    void setUp() {
        properties = new GenesysProperties();
        properties.setLoginUrl("https://login.example.com");
        properties.setTokenPath("/oauth/token");
        properties.setClientId("client123");
        properties.setClientSecret("secret456");
        
        tokenProvider = new GenesysOAuth2TokenProvider(restClient, properties);
    }

    @Test
    void getAccessToken_shouldFetchNewTokenWhenMissing() {
        // Arrange
        GenesysOAuth2TokenProvider.TokenResponse mockResponse = new GenesysOAuth2TokenProvider.TokenResponse();
        mockResponse.setAccessToken("new-token");
        mockResponse.setExpiresIn(3600);

        when(restClient.post()).thenReturn(requestBodyUriSpec);
        when(requestBodyUriSpec.uri("https://login.example.com/oauth/token")).thenReturn(requestBodySpec);
        when(requestBodySpec.headers(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.body(any(Object.class))).thenReturn(requestBodySpec);
        when(requestBodySpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.body(GenesysOAuth2TokenProvider.TokenResponse.class))
                .thenReturn(mockResponse);

        // Act
        String token = tokenProvider.getAccessToken();

        // Assert
        assertEquals("new-token", token);
        verify(restClient, times(1)).post();
    }

    @Test
    void getAccessToken_shouldReturnCachedTokenWhenValid() {
        // Arrange
        GenesysOAuth2TokenProvider.TokenResponse mockResponse = new GenesysOAuth2TokenProvider.TokenResponse();
        mockResponse.setAccessToken("new-token");
        mockResponse.setExpiresIn(3600);

        when(restClient.post()).thenReturn(requestBodyUriSpec);
        when(requestBodyUriSpec.uri("https://login.example.com/oauth/token")).thenReturn(requestBodySpec);
        when(requestBodySpec.headers(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.body(any(Object.class))).thenReturn(requestBodySpec);
        when(requestBodySpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.body(GenesysOAuth2TokenProvider.TokenResponse.class))
                .thenReturn(mockResponse);

        // Act
        String firstToken = tokenProvider.getAccessToken();
        String secondToken = tokenProvider.getAccessToken();

        // Assert
        assertEquals("new-token", firstToken);
        assertEquals("new-token", secondToken);
        // Only one REST call should have been made
        verify(restClient, times(1)).post();
    }

    @Test
    void invalidateToken_shouldForceNewTokenFetch() {
        // Arrange
        GenesysOAuth2TokenProvider.TokenResponse mockResponse1 = new GenesysOAuth2TokenProvider.TokenResponse();
        mockResponse1.setAccessToken("token-1");
        mockResponse1.setExpiresIn(3600);

        GenesysOAuth2TokenProvider.TokenResponse mockResponse2 = new GenesysOAuth2TokenProvider.TokenResponse();
        mockResponse2.setAccessToken("token-2");
        mockResponse2.setExpiresIn(3600);

        when(restClient.post()).thenReturn(requestBodyUriSpec);
        when(requestBodyUriSpec.uri("https://login.example.com/oauth/token")).thenReturn(requestBodySpec);
        when(requestBodySpec.headers(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.body(any(Object.class))).thenReturn(requestBodySpec);
        when(requestBodySpec.retrieve()).thenReturn(responseSpec);
        
        when(responseSpec.body(GenesysOAuth2TokenProvider.TokenResponse.class))
                .thenReturn(mockResponse1)
                .thenReturn(mockResponse2);

        // Act
        String firstToken = tokenProvider.getAccessToken();
        tokenProvider.invalidateToken();
        String secondToken = tokenProvider.getAccessToken();

        // Assert
        assertEquals("token-1", firstToken);
        assertEquals("token-2", secondToken);
        verify(restClient, times(2)).post();
    }

    @Test
    void getAccessToken_shouldThrowExceptionOnRestFailure() {
        // Arrange
        when(restClient.post()).thenReturn(requestBodyUriSpec);
        when(requestBodyUriSpec.uri("https://login.example.com/oauth/token")).thenReturn(requestBodySpec);
        when(requestBodySpec.headers(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.body(any(Object.class))).thenReturn(requestBodySpec);
        when(requestBodySpec.retrieve()).thenThrow(new RestClientException("Connection refused"));

        // Act & Assert
        GenesysRestException exception = assertThrows(GenesysRestException.class, () -> {
            tokenProvider.getAccessToken();
        });
        
        assertTrue(exception.getMessage().contains("Failed to obtain Genesys"));
    }
}
