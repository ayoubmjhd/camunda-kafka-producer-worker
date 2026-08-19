package com.camunda.kafka.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration properties for the Genesys Cloud REST Worker.
 *
 * <p>Properties are read from the {@code genesys} prefix in application.yml.
 * Required fields are validated at startup — if any are missing, the
 * application will fail fast with a clear error message.
 */
@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "genesys")
public class GenesysProperties {

    /** Genesys Cloud API base URL (e.g., https://api.mypurecloud.de) */
    @NotBlank(message = "genesys.api-base-url is required")
    private String apiBaseUrl;

    /** Genesys Cloud login URL for OAuth (e.g., https://login.mypurecloud.de) */
    @NotBlank(message = "genesys.login-url is required")
    private String loginUrl;

    /** OAuth2 Client ID */
    @NotBlank(message = "genesys.client-id is required")
    private String clientId;

    /** OAuth2 Client Secret */
    @NotBlank(message = "genesys.client-secret is required")
    private String clientSecret;

    /** OAuth2 scope (optional, e.g., "api" for some providers) */
    private String scope;

    /** Token endpoint path appended to loginUrl (default: /oauth/token) */
    private String tokenPath = "/oauth/token";

    /** HTTP connect timeout in seconds (default: 30) */
    @Positive
    private int connectTimeoutSeconds = 30;

    /** HTTP read timeout in seconds (default: 120 — generous to avoid 408) */
    @Positive
    private int readTimeoutSeconds = 120;

}
