package com.camunda.kafka.infrastructure.security;

import com.camunda.kafka.application.port.outbound.GenesysTokenProvider;
import com.camunda.kafka.config.GenesysProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;

/**
 * Configures Genesys-specific {@link RestClient} beans with generous timeouts
 * (default 120s read) to avoid the HTTP 408 timeout issues that the
 * Camunda REST Connector was experiencing.
 *
 * <p>Two beans are produced:
 * <ul>
 *   <li>{@code genesysAuthRestClient} — for the token endpoint (no auth interceptor)</li>
 *   <li>{@code genesysApiRestClient} — for API calls (auto-injects Bearer token)</li>
 * </ul>
 *
 * <p>OAuth2 client-credentials flow is managed by {@link com.camunda.kafka.adapter.outbound.genesys.GenesysOAuth2TokenProvider},
 * which provides the Bearer token injected by the API client's interceptor.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(GenesysProperties.class)
public class GenesysRestClientConfig {

    private final GenesysProperties properties;

    /**
     * RestClient for the OAuth2 token endpoint.
     * No auth interceptor — the token provider handles Basic auth internally.
     */
    @Bean
    public RestClient genesysAuthRestClient() {
        log.info("Configuring Genesys Auth RestClient: connectTimeout={}s, readTimeout={}s",
                properties.getConnectTimeoutSeconds(), properties.getReadTimeoutSeconds());

        return RestClient.builder()
                .requestFactory(buildRequestFactory())
                .build();
    }

    /**
     * RestClient for Genesys Cloud API calls.
     * Includes an interceptor that auto-injects the Bearer token from the token provider.
     */
    @Bean
    public RestClient genesysApiRestClient(GenesysTokenProvider tokenProvider) {
        log.info("Configuring Genesys API RestClient: connectTimeout={}s, readTimeout={}s, apiBaseUrl={}",
                properties.getConnectTimeoutSeconds(), properties.getReadTimeoutSeconds(),
                properties.getApiBaseUrl());

        return RestClient.builder()
                .requestFactory(buildRequestFactory())
                .requestInterceptor((request, body, execution) -> {
                    request.getHeaders().setBearerAuth(tokenProvider.getAccessToken());
                    return execution.execute(request, body);
                })
                .build();
    }

    /**
     * Builds a {@link JdkClientHttpRequestFactory} with the configured timeouts.
     * Reused by both the auth and API RestClient beans.
     */
    private JdkClientHttpRequestFactory buildRequestFactory() {
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(properties.getConnectTimeoutSeconds()))
                .build();

        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(Duration.ofSeconds(properties.getReadTimeoutSeconds()));
        return factory;
    }
}
