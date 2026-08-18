package com.camunda.kafka.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

/**
 * Spring configuration that registers the Genesys-specific beans.
 *
 * <p>Creates a {@link RestTemplate} with generous timeouts
 * (default 120s read) to avoid the HTTP 408 timeout issues that the
 * Camunda REST Connector was experiencing.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(GenesysProperties.class)
public class GenesysConfig {

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
