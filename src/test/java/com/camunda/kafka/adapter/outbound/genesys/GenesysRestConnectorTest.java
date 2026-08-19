package com.camunda.kafka.adapter.outbound.genesys;

import com.camunda.kafka.config.GenesysProperties;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestRequest;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestResponse;
import com.camunda.kafka.exception.GenesysRestException;
import com.camunda.kafka.application.port.outbound.GenesysTokenProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.*;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Integration-style test for {@link GenesysRestConnector} using WireMock
 * to simulate the Genesys Cloud API. The OAuth2 token is mocked via
 * a stubbed {@link GenesysTokenProvider}.
 */
class GenesysRestConnectorTest {

    private static WireMockServer wireMockServer;
    private GenesysRestConnector connector;
    private GenesysTokenProvider authService;

    @BeforeAll
    static void startWireMock() {
        wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        wireMockServer.start();
        WireMock.configureFor("localhost", wireMockServer.port());
    }

    @AfterAll
    static void stopWireMock() {
        wireMockServer.stop();
    }

    @BeforeEach
    void setUp() {
        wireMockServer.resetAll();

        // Mock the auth service to return a fake token
        authService = mock(GenesysTokenProvider.class);
        when(authService.getAccessToken()).thenReturn("mock-oauth2-token");

        // Configure properties to point at WireMock
        GenesysProperties properties = new GenesysProperties();
        properties.setApiBaseUrl("http://localhost:" + wireMockServer.port());
        properties.setLoginUrl("http://localhost:" + wireMockServer.port());
        properties.setClientId("test-client-id");
        properties.setClientSecret("test-client-secret");
        properties.setConnectTimeoutSeconds(5);
        properties.setReadTimeoutSeconds(10);

        // Use SimpleClientHttpRequestFactory (HTTP/1.1) to avoid JDK HttpClient
        // HTTP/2 RST_STREAM issues with WireMock
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofSeconds(5));
        requestFactory.setReadTimeout(Duration.ofSeconds(10));

        RestTemplate restTemplate = new RestTemplateBuilder()
                .requestFactory(() -> requestFactory)
                .build();

        connector = new GenesysRestConnector(
                restTemplate,
                new RestTemplateBuilder(),
                properties,
                authService,
                new ObjectMapper()
        );
    }

    @Test
    @DisplayName("GET request returns 200 with JSON body")
    void get_Success_ReturnsJsonBody() {
        // Arrange – WireMock stub
        stubFor(get(urlEqualTo("/api/v2/users/12345"))
                .withHeader("Authorization", equalTo("Bearer mock-oauth2-token"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"id\":\"12345\",\"name\":\"John Doe\"}")));

        GenesysRestRequest request = GenesysRestRequest.builder()
                .url("/api/v2/users/12345")
                .method("GET")
                .build();

        // Act
        GenesysRestResponse response = connector.call(request);

        // Assert
        assertEquals(200, response.getStatus());
        assertNotNull(response.getBody());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getBody();
        assertEquals("12345", body.get("id"));
        assertEquals("John Doe", body.get("name"));

        // Verify Bearer token was sent
        verify(1, getRequestedFor(urlEqualTo("/api/v2/users/12345"))
                .withHeader("Authorization", equalTo("Bearer mock-oauth2-token")));
    }

    @Test
    @DisplayName("POST request with JSON body returns 201")
    void post_WithBody_Returns201() {
        // Arrange
        stubFor(post(urlEqualTo("/api/v2/conversations/emails/agentless"))
                .withHeader("Authorization", equalTo("Bearer mock-oauth2-token"))
                .withHeader("Content-Type", containing("application/json"))
                .willReturn(aResponse()
                        .withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"conversationId\":\"conv-001\"}")));

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("emailAddress", "test@example.com");
        requestBody.put("subject", "Test Email");

        GenesysRestRequest request = GenesysRestRequest.builder()
                .url("/api/v2/conversations/emails/agentless")
                .method("POST")
                .body(requestBody)
                .build();

        // Act
        GenesysRestResponse response = connector.call(request);

        // Assert
        assertEquals(201, response.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getBody();
        assertEquals("conv-001", body.get("conversationId"));
    }

    @Test
    @DisplayName("GET with query parameters appends them to URL")
    void get_WithQueryParams_AppendsToUrl() {
        // Arrange
        stubFor(get(urlPathEqualTo("/api/v2/users"))
                .withQueryParam("pageSize", equalTo("25"))
                .withQueryParam("pageNumber", equalTo("1"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"entities\":[],\"total\":0}")));

        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("pageSize", 25);
        queryParams.put("pageNumber", 1);

        GenesysRestRequest request = GenesysRestRequest.builder()
                .url("/api/v2/users")
                .method("GET")
                .queryParameters(queryParams)
                .build();

        // Act
        GenesysRestResponse response = connector.call(request);

        // Assert
        assertEquals(200, response.getStatus());
    }

    @Test
    @DisplayName("Custom headers are forwarded (but not Authorization)")
    void get_WithCustomHeaders_ForwardsHeaders() {
        // Arrange
        stubFor(get(urlEqualTo("/api/v2/analytics"))
                .withHeader("X-Custom-Header", equalTo("custom-value"))
                .withHeader("Authorization", equalTo("Bearer mock-oauth2-token"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody("{}")));

        Map<String, String> customHeaders = new HashMap<>();
        customHeaders.put("X-Custom-Header", "custom-value");
        // Trying to override Authorization should be ignored
        customHeaders.put("Authorization", "Bearer should-be-ignored");

        GenesysRestRequest request = GenesysRestRequest.builder()
                .url("/api/v2/analytics")
                .method("GET")
                .headers(customHeaders)
                .build();

        // Act
        GenesysRestResponse response = connector.call(request);

        // Assert
        assertEquals(200, response.getStatus());
        // The Authorization header should be the OAuth token, not the custom one
        verify(1, getRequestedFor(urlEqualTo("/api/v2/analytics"))
                .withHeader("Authorization", equalTo("Bearer mock-oauth2-token")));
    }

    @Test
    @DisplayName("401 triggers token refresh and retry")
    void call_401_RefreshesTokenAndRetries() {
        // Arrange – first call returns 401, after token refresh second call succeeds
        stubFor(get(urlEqualTo("/api/v2/users"))
                .inScenario("token-refresh")
                .whenScenarioStateIs("Started")
                .willReturn(aResponse().withStatus(401).withBody("{\"message\":\"Unauthorized\"}"))
                .willSetStateTo("token-refreshed"));

        stubFor(get(urlEqualTo("/api/v2/users"))
                .inScenario("token-refresh")
                .whenScenarioStateIs("token-refreshed")
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"id\":\"refreshed\"}")));

        // After invalidation, return a new token
        when(authService.getAccessToken())
                .thenReturn("mock-oauth2-token")
                .thenReturn("refreshed-oauth2-token");

        GenesysRestRequest request = GenesysRestRequest.builder()
                .url("/api/v2/users")
                .method("GET")
                .build();

        // Act
        GenesysRestResponse response = connector.call(request);

        // Assert
        assertEquals(200, response.getStatus());
        org.mockito.Mockito.verify(authService).invalidateToken();
    }

    @Test
    @DisplayName("408 timeout throws GenesysRestException for Zeebe retry")
    void call_408_ThrowsRetryableException() {
        // Arrange
        stubFor(get(urlEqualTo("/api/v2/slow-endpoint"))
                .willReturn(aResponse()
                        .withStatus(408)
                        .withBody("{\"message\":\"Request Timeout\"}")));

        GenesysRestRequest request = GenesysRestRequest.builder()
                .url("/api/v2/slow-endpoint")
                .method("GET")
                .build();

        // Act & Assert
        GenesysRestException ex = assertThrows(GenesysRestException.class, () ->
                connector.call(request));

        assertTrue(ex.getMessage().contains("408"));
        assertTrue(ex.getMessage().contains("retryable"));
    }

    @Test
    @DisplayName("429 rate limit throws GenesysRestException for Zeebe retry")
    void call_429_ThrowsRetryableException() {
        // Arrange
        stubFor(get(urlEqualTo("/api/v2/rate-limited"))
                .willReturn(aResponse()
                        .withStatus(429)
                        .withBody("{\"message\":\"Too Many Requests\"}")));

        GenesysRestRequest request = GenesysRestRequest.builder()
                .url("/api/v2/rate-limited")
                .method("GET")
                .build();

        // Act & Assert
        GenesysRestException ex = assertThrows(GenesysRestException.class, () ->
                connector.call(request));

        assertTrue(ex.getMessage().contains("429"));
    }

    @Test
    @DisplayName("5xx server error throws GenesysRestException for Zeebe retry")
    void call_500_ThrowsRetryableException() {
        // Arrange
        stubFor(get(urlEqualTo("/api/v2/server-error"))
                .willReturn(aResponse()
                        .withStatus(500)
                        .withBody("{\"message\":\"Internal Server Error\"}")));

        GenesysRestRequest request = GenesysRestRequest.builder()
                .url("/api/v2/server-error")
                .method("GET")
                .build();

        // Act & Assert
        assertThrows(GenesysRestException.class, () ->
                connector.call(request));
    }

    @Test
    @DisplayName("404 client error returns response (not exception)")
    void call_404_ReturnsResponseWithStatus() {
        // Arrange
        stubFor(get(urlEqualTo("/api/v2/not-found"))
                .willReturn(aResponse()
                        .withStatus(404)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"message\":\"Not Found\"}")));

        GenesysRestRequest request = GenesysRestRequest.builder()
                .url("/api/v2/not-found")
                .method("GET")
                .build();

        // Act
        GenesysRestResponse response = connector.call(request);

        // Assert – 404 is returned as a response, not thrown
        assertEquals(404, response.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getBody();
        assertEquals("Not Found", body.get("message"));
    }

    @Test
    @DisplayName("Full URL (not starting with /) is used as-is")
    void call_FullUrl_UsedAsIs() {
        // Arrange – use the WireMock URL directly as a "full URL"
        String fullUrl = "http://localhost:" + wireMockServer.port() + "/external/api";

        stubFor(get(urlEqualTo("/external/api"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody("{\"source\":\"external\"}")));

        GenesysRestRequest request = GenesysRestRequest.builder()
                .url(fullUrl)
                .method("GET")
                .build();

        // Act
        GenesysRestResponse response = connector.call(request);

        // Assert
        assertEquals(200, response.getStatus());
    }

    @Test
    @DisplayName("DELETE request works correctly")
    void delete_ReturnsNoContent() {
        // Arrange
        stubFor(delete(urlEqualTo("/api/v2/users/12345"))
                .willReturn(aResponse().withStatus(204)));

        GenesysRestRequest request = GenesysRestRequest.builder()
                .url("/api/v2/users/12345")
                .method("DELETE")
                .build();

        // Act
        GenesysRestResponse response = connector.call(request);

        // Assert
        assertEquals(204, response.getStatus());
    }

    @Test
    @DisplayName("PUT request with body works correctly")
    void put_WithBody_Returns200() {
        // Arrange
        stubFor(put(urlEqualTo("/api/v2/users/12345"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"id\":\"12345\",\"name\":\"Updated Name\"}")));

        Map<String, Object> updateBody = new HashMap<>();
        updateBody.put("name", "Updated Name");

        GenesysRestRequest request = GenesysRestRequest.builder()
                .url("/api/v2/users/12345")
                .method("PUT")
                .body(updateBody)
                .build();

        // Act
        GenesysRestResponse response = connector.call(request);

        // Assert
        assertEquals(200, response.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getBody();
        assertEquals("Updated Name", body.get("name"));
    }
}
