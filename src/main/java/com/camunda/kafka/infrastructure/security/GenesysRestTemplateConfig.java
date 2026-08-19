package com.camunda.kafka.infrastructure.security;

import com.camunda.kafka.config.GenesysProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

/**
 * Configures the Genesys-specific {@link RestTemplate} bean with generous timeouts
 * (default 120s read) to avoid the HTTP 408 timeout issues that the
 * Camunda REST Connector was experiencing.
 *
 * <p>OAuth2 client-credentials flow is managed by {@link com.camunda.kafka.service.GenesysAuthService},
 * which injects the Bearer token into every request via the
 * {@link com.camunda.kafka.adapter.outbound.genesys.GenesysRestConnector}.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(GenesysProperties.class)
public class GenesysRestTemplateConfig {

    private final GenesysProperties properties;

    @Bean
    public RestTemplate genesysRestTemplate(RestTemplateBuilder builder) {
        log.info("Configuring Genesys RestTemplate: connectTimeout={}s, readTimeout={}s, apiBaseUrl={}",
                properties.getConnectTimeoutSeconds(), properties.getReadTimeoutSeconds(),
                properties.getApiBaseUrl());

        return builder
                .requestFactory(org.springframework.http.client.JdkClientHttpRequestFactory.class)
                .setConnectTimeout(Duration.ofSeconds(properties.getConnectTimeoutSeconds()))
                .setReadTimeout(Duration.ofSeconds(properties.getReadTimeoutSeconds()))
                .build();
    }
}
