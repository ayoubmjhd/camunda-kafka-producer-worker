package com.camunda.kafka.service;

import com.camunda.kafka.config.GenesysProperties;
import com.camunda.kafka.exception.GenesysRestException;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

/**
 * Manages the OAuth2 Client Credentials flow for Genesys Cloud.
 *
 * <p>Handles token acquisition, caching, and automatic refresh.
 * Optimizes read paths using lock-free atomic checks with a Java 17 Record wrapper,
 * avoiding thread synchronization bottleneck on high-throughput jobs.
 */
@Slf4j
@Service
public class GenesysAuthService {

    private static final long TOKEN_REFRESH_BUFFER_SECONDS = 60;

    private final RestTemplate restTemplate;
    private final GenesysProperties config;

    private record CachedToken(String token, Instant expiry) {}

    private volatile CachedToken cachedToken = new CachedToken(null, Instant.EPOCH);

    public GenesysAuthService(RestTemplate restTemplate, GenesysProperties config) {
        this.restTemplate = restTemplate;
        this.config = config;
    }

    /**
     * Returns a valid access token, refreshing if needed.
     * Lock-free read path under normal operation, synchronized only during token refresh.
     *
     * @return a valid Bearer access token
     * @throws GenesysRestException if token acquisition fails
     */
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

            log.info("Genesys OAuth token expired or missing, requesting new token...");
            refreshToken();
            return this.cachedToken.token();
        }
    }

    /**
     * Forces a token refresh. Call this when the API returns 401
     * to handle token invalidation by Genesys.
     */
    public synchronized void invalidateToken() {
        log.info("Invalidating Genesys OAuth token");
        this.cachedToken = new CachedToken(null, Instant.EPOCH);
    }

    private boolean isTokenValid(CachedToken current) {
        return current.token() != null
                && Instant.now().plusSeconds(TOKEN_REFRESH_BUFFER_SECONDS).isBefore(current.expiry());
    }

    private void refreshToken() {
        String tokenUrl = config.getLoginUrl() + "/oauth/token";

        // Build Basic auth header: Base64(clientId:clientSecret)
        String credentials = config.getClientId() + ":" + config.getClientSecret();
        String encodedCredentials = Base64.getEncoder()
                .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        headers.set(HttpHeaders.AUTHORIZATION, "Basic " + encodedCredentials);

        MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
        body.add("grant_type", "client_credentials");

        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(body, headers);

        try {
            ResponseEntity<TokenResponse> response = restTemplate.exchange(
                    tokenUrl, HttpMethod.POST, request, TokenResponse.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                TokenResponse tokenResponse = response.getBody();
                this.cachedToken = new CachedToken(
                        tokenResponse.getAccessToken(),
                        Instant.now().plusSeconds(tokenResponse.getExpiresIn())
                );
                log.info("Genesys OAuth token acquired, expires in {}s", tokenResponse.getExpiresIn());
            } else {
                throw new GenesysRestException(
                        "Failed to obtain Genesys token: HTTP " + response.getStatusCode());
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
