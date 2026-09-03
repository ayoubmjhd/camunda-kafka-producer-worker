package com.camunda.kafka.adapter.outbound.genesys;

import com.camunda.kafka.application.port.outbound.GenesysTokenProvider;
import com.camunda.kafka.config.GenesysProperties;
import com.camunda.kafka.exception.GenesysRestException;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

/**
 * OAuth2 Client Credentials token provider for Genesys Cloud.
 *
 * <p>Handles token acquisition, caching, and automatic refresh.
 * Optimizes read paths using lock-free atomic checks with a Java 17 Record wrapper,
 * avoiding thread synchronization bottleneck on high-throughput jobs.
 */
@Slf4j
@Component
public class GenesysOAuth2TokenProvider implements GenesysTokenProvider {

    private static final long TOKEN_REFRESH_BUFFER_SECONDS = 60;

    private final RestClient restClient;
    private final GenesysProperties config;

    private record CachedToken(String token, Instant expiry) {}

    private volatile CachedToken cachedToken = new CachedToken(null, Instant.EPOCH);

    public GenesysOAuth2TokenProvider(RestClient genesysAuthRestClient, GenesysProperties config) {
        this.restClient = genesysAuthRestClient;
        this.config = config;
    }

    /**
     * Returns a valid access token, refreshing if needed.
     * Lock-free read path under normal operation, synchronized only during token refresh.
     *
     * @return a valid Bearer access token
     * @throws GenesysRestException if token acquisition fails
     */
    @Override
    public String getAccessToken() {
        CachedToken current = this.cachedToken;
        if (isTokenValid(current)) {
            return current.token();
        }

        synchronized (this) {
            current = this.cachedToken;
            // Double-check lock validation
            if (isTokenValid(current)) {
                return current.token();
            }

            log.debug("Genesys OAuth token expired or missing, requesting new token...");
            refreshToken();
            return this.cachedToken.token();
        }
    }

    /**
     * Forces a token refresh. Call this when the API returns 401
     * to handle token invalidation by Genesys.
     */
    @Override
    public synchronized void invalidateToken() {
        log.info("Invalidating Genesys OAuth token");
        this.cachedToken = new CachedToken(null, Instant.EPOCH);
    }

    private boolean isTokenValid(CachedToken current) {
        return current.token() != null
                && Instant.now().plusSeconds(TOKEN_REFRESH_BUFFER_SECONDS).isBefore(current.expiry());
    }

    private void refreshToken() {
        String tokenUrl = config.getLoginUrl() + config.getTokenPath();

        // Build Basic auth header: Base64(clientId:clientSecret)
        String credentials = config.getClientId() + ":" + config.getClientSecret();
        String encodedCredentials = Base64.getEncoder()
                .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));

        MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
        body.add("grant_type", "client_credentials");

        // Include scope if configured (required by some OAuth2 providers)
        if (config.getScope() != null && !config.getScope().isBlank()) {
            body.add("scope", config.getScope());
        }

        try {
            TokenResponse tokenResponse = restClient.post()
                    .uri(tokenUrl)
                    .headers(h -> {
                        h.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
                        h.set(HttpHeaders.AUTHORIZATION, "Basic " + encodedCredentials);
                    })
                    .body(body)
                    .retrieve()
                    .body(TokenResponse.class);

            if (tokenResponse != null && tokenResponse.getAccessToken() != null) {
                this.cachedToken = new CachedToken(
                        tokenResponse.getAccessToken(),
                        Instant.now().plusSeconds(tokenResponse.getExpiresIn())
                );
                log.debug("Genesys OAuth token acquired, expires in {}s", tokenResponse.getExpiresIn());
            } else {
                throw new GenesysRestException("Failed to obtain Genesys token: empty response");
            }
        } catch (RestClientException e) {
            throw new GenesysRestException("Failed to obtain Genesys OAuth token: " + e.getMessage(), e);
        }
    }

    @Data
    private static class TokenResponse {
        @JsonProperty("access_token")
        private String accessToken;

        @JsonProperty("token_type")
        private String tokenType;

        @JsonProperty("expires_in")
        private long expiresIn;
    }
}
