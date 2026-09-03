package com.camunda.kafka.adapter.outbound.genesys;

import com.camunda.kafka.config.GenesysProperties;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestRequest;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestResponse;
import com.camunda.kafka.application.port.outbound.GenesysTokenProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Live integration test that exercises the full OAuth2 client-credentials
 * flow against the public Duende IdentityServer demo.
 *
 * <p>This test proves that {@link GenesysOAuth2TokenProvider} can obtain a real
 * OAuth2 token, and {@link GenesysRestConnector} can use it to call
 * a protected API endpoint — the exact same flow used for Genesys Cloud.
 *
 * <p><strong>Requires network access.</strong> Disabled by default;
 * run with {@code -Dtest=GenesysRestLiveIntegrationTest} to execute.
 *
 * <p>Demo API details:
 * <ul>
 *   <li>Token URL: https://demo.duendesoftware.com/connect/token</li>
 *   <li>Client ID: m2m / Secret: secret / Scope: api</li>
 *   <li>API: GET https://demo.duendesoftware.com/api/test</li>
 * </ul>
 */
@Tag("live")
@DisplayName("Live Integration Test — Duende Demo OAuth2 API")
class GenesysRestLiveIntegrationTest {

    private GenesysRestConnector connector;
    private GenesysTokenProvider authService;
    private GenesysProperties properties;

    @BeforeEach
    void setUp() {
        // Configure properties to point at Duende demo
        properties = new GenesysProperties();
        properties.setApiBaseUrl("https://demo.duendesoftware.com");
        properties.setLoginUrl("https://demo.duendesoftware.com");
        properties.setTokenPath("/connect/token");
        properties.setClientId("m2m");
        properties.setClientSecret("secret");
        properties.setScope("api");
        properties.setConnectTimeoutSeconds(10);
        properties.setReadTimeoutSeconds(30);

        // RestClient for the auth service (token endpoint)
        SimpleClientHttpRequestFactory authFactory = new SimpleClientHttpRequestFactory();
        authFactory.setConnectTimeout(Duration.ofSeconds(10));
        authFactory.setReadTimeout(Duration.ofSeconds(30));
        RestClient authRestClient = RestClient.builder()
                .requestFactory(authFactory)
                .build();

        authService = new GenesysOAuth2TokenProvider(authRestClient, properties);

        // RestClient for the connector (API calls — with auth interceptor)
        SimpleClientHttpRequestFactory apiFactory = new SimpleClientHttpRequestFactory();
        apiFactory.setConnectTimeout(Duration.ofSeconds(10));
        apiFactory.setReadTimeout(Duration.ofSeconds(30));
        RestClient apiRestClient = RestClient.builder()
                .requestFactory(apiFactory)
                .requestInterceptor((request, body, execution) -> {
                    request.getHeaders().setBearerAuth(authService.getAccessToken());
                    return execution.execute(request, body);
                })
                .build();

        connector = new GenesysRestConnector(
                apiRestClient,
                properties,
                authService,
                new ObjectMapper()
        );
    }

    @Test
    @DisplayName("Full OAuth2 flow: obtain token → call protected API → get response")
    void fullOAuth2Flow_ObtainsTokenAndCallsApi() {
        // Step 1: Verify we can get a token
        String token = authService.getAccessToken();
        assertNotNull(token, "OAuth2 token should not be null");
        assertFalse(token.isBlank(), "OAuth2 token should not be blank");
        System.out.println("✅ OAuth2 token obtained (length=" + token.length() + ")");

        // Step 2: Call the protected API via the connector
        GenesysRestRequest request = GenesysRestRequest.builder()
                .url("/api/test")
                .method("GET")
                .build();

        GenesysRestResponse response = connector.call(request);

        // Step 3: Validate the response
        System.out.println("✅ API response: status=" + response.getStatus()
                + ", body=" + response.getBody());

        assertEquals(200, response.getStatus(),
                "Protected API should return 200 with valid Bearer token");
        assertNotNull(response.getBody(), "Response body should not be null");
    }

    @Test
    @DisplayName("Token caching: second call reuses cached token")
    void tokenCaching_SecondCallReusesCachedToken() {
        // Get token twice — second should be cached
        String token1 = authService.getAccessToken();
        String token2 = authService.getAccessToken();

        assertEquals(token1, token2, "Second call should return the cached token");
        System.out.println("✅ Token caching works — same token returned on second call");
    }

    @Test
    @DisplayName("POST request to protected API")
    void post_ToProtectedApi() {
        // Some OAuth2 demo APIs accept POST as well
        GenesysRestRequest request = GenesysRestRequest.builder()
                .url("/api/test")
                .method("POST")
                .body(Map.of("test", "data"))
                .build();

        // This might return 404 or 405 since POST may not be supported,
        // but the point is proving the auth flow works end-to-end
        GenesysRestResponse response = connector.call(request);
        System.out.println("✅ POST response: status=" + response.getStatus()
                + ", body=" + response.getBody());

        // We just verify we got a valid HTTP response (not an auth error)
        assertTrue(response.getStatus() != 401,
                "Should not get 401 — token should be valid");
    }
}
