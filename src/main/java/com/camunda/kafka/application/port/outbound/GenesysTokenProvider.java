package com.camunda.kafka.application.port.outbound;

/**
 * Outbound port for obtaining OAuth2 access tokens.
 * The concrete implementation handles caching, refresh, and the
 * client-credentials flow against the configured identity provider.
 */
public interface GenesysTokenProvider {

    /**
     * Returns a valid access token, refreshing if needed.
     *
     * @return a valid Bearer access token
     */
    String getAccessToken();

    /**
     * Forces token invalidation. Call this when the API returns 401
     * to trigger a fresh token on the next {@link #getAccessToken()} call.
     */
    void invalidateToken();
}
