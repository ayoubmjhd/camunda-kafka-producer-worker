package com.camunda.kafka.adapter.outbound.genesys;

import com.camunda.kafka.application.port.outbound.GenesysRestOutboundPort;
import com.camunda.kafka.config.GenesysProperties;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestRequest;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestResponse;
import com.camunda.kafka.exception.GenesysRestException;
import com.camunda.kafka.application.port.outbound.GenesysTokenProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.*;
import org.springframework.web.util.UriComponentsBuilder;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Concrete implementation of the outbound port that uses Spring {@link RestTemplate}
 * to perform OAuth2-protected HTTP calls to Genesys Cloud.
 *
 * <p>Handles:
 * <ul>
 *   <li>OAuth2 Bearer token injection (auto-refreshed)</li>
 *   <li>URL resolution (relative paths get the base URL prepended)</li>
 *   <li>Query parameter handling (lists, objects, primitives)</li>
 *   <li>Throws specific exceptions for 408, 429, and 5xx so Zeebe can handle retries</li>
 *   <li>Per-request timeout overrides (safely cached to prevent leaks)</li>
 *   <li>JSON response parsing</li>
 * </ul>
 */
@Slf4j
@Component
public class GenesysRestConnector implements GenesysRestOutboundPort {

    private final RestTemplate defaultRestTemplate;
    private final RestTemplateBuilder restTemplateBuilder;
    private final GenesysProperties config;
    private final GenesysTokenProvider tokenProvider;
    private final ObjectMapper objectMapper;
    private final Map<String, RestTemplate> restTemplateCache = Collections.synchronizedMap(
            new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, RestTemplate> eldest) {
                    return size() > 50; // Max 50 cached custom RestTemplates
                }
            }
    );

    public GenesysRestConnector(RestTemplate genesysRestTemplate,
                                RestTemplateBuilder restTemplateBuilder,
                                GenesysProperties config,
                                GenesysTokenProvider tokenProvider,
                                ObjectMapper objectMapper) {
        this.defaultRestTemplate = genesysRestTemplate;
        this.restTemplateBuilder = restTemplateBuilder;
        this.config = config;
        this.tokenProvider = tokenProvider;
        this.objectMapper = objectMapper;
    }

    @Override
    public GenesysRestResponse call(GenesysRestRequest request) {
        // 1. Resolve URL
        String resolvedUrl = resolveUrl(request.getUrl(), request.getQueryParameters());

        // 2. Build headers with Bearer token
        HttpHeaders headers = buildHeaders(request.getHeaders());

        // 3. Process the request body (convert Map to MultiValueMap for form/multipart requests)
        Object requestBody = request.getBody();
        if (requestBody instanceof Map && headers.getContentType() != null) {
            MediaType contentType = headers.getContentType();
            if (MediaType.APPLICATION_FORM_URLENCODED.includes(contentType)
                    || MediaType.MULTIPART_FORM_DATA.includes(contentType)) {
                MultiValueMap<String, Object> formParams = new LinkedMultiValueMap<>();
                ((Map<?, ?>) requestBody).forEach((k, v) -> {
                    if (v != null) {
                        formParams.add(k.toString(), v);
                    }
                });
                requestBody = formParams;
            }
        }

        HttpEntity<Object> httpRequest = (requestBody != null)
                ? new HttpEntity<>(requestBody, headers)
                : new HttpEntity<>(headers);
        HttpMethod httpMethod = HttpMethod.valueOf(request.getMethod().toUpperCase());

        // 4. Get the right RestTemplate (with caching for overrides)
        RestTemplate targetRestTemplate = getRestTemplate(
                request.getConnectionTimeoutInSeconds(), request.getReadTimeoutInSeconds());

        // 5. Execute request (letting Zeebe handle retries for failures)
        return executeRequest(targetRestTemplate, resolvedUrl, httpMethod, httpRequest);
    }

    private GenesysRestResponse executeRequest(
            RestTemplate targetRestTemplate, String url, HttpMethod method, HttpEntity<Object> request) {

        try {
            log.debug("Genesys API call: {} {}", method, url);
            ResponseEntity<String> response = targetRestTemplate.exchange(url, method, request, String.class);
            return buildResponse(response);

        } catch (HttpClientErrorException e) {
            int statusCode = e.getStatusCode().value();

            // 401: token might be expired — invalidate, refresh, and retry EXACTLY ONCE
            if (statusCode == 401) {
                log.warn("Genesys API returned 401, invalidating token and retrying once...");
                tokenProvider.invalidateToken();
                // Rebuild headers with fresh token
                HttpHeaders freshHeaders = buildHeaders(extractCustomHeaders(request.getHeaders()));
                HttpEntity<Object> retryRequest = (request.getBody() != null)
                        ? new HttpEntity<>(request.getBody(), freshHeaders)
                        : new HttpEntity<>(freshHeaders);

                try {
                    ResponseEntity<String> retryResponse =
                            targetRestTemplate.exchange(url, method, retryRequest, String.class);
                    return buildResponse(retryResponse);
                } catch (HttpStatusCodeException retryEx) {
                    if (retryEx.getStatusCode().is4xxClientError()
                            && retryEx.getStatusCode().value() != 408
                            && retryEx.getStatusCode().value() != 429) {
                        log.error("Genesys API client error after token refresh {}: {}",
                                retryEx.getStatusCode().value(), retryEx.getResponseBodyAsString());
                        return buildErrorResponse(retryEx);
                    }
                    throw new GenesysRestException(
                            "Genesys API error after refresh " + retryEx.getStatusCode().value()
                                    + ": " + retryEx.getResponseBodyAsString(), retryEx);
                }
            }

            // 408 (timeout) and 429 (rate limit) should trigger Zeebe job retry
            if (statusCode == 408 || statusCode == 429) {
                throw new GenesysRestException(
                        "Genesys API returned " + statusCode + " (retryable): "
                                + e.getResponseBodyAsString(), e);
            }

            // Other 4xx client errors — return as response (let BPMN handle it)
            log.error("Genesys API client error {}: {}", statusCode, e.getResponseBodyAsString());
            return buildErrorResponse(e);

        } catch (HttpServerErrorException e) {
            // 5xx server errors — throw exception so Zeebe can retry the job
            throw new GenesysRestException(
                    "Genesys API server error " + e.getStatusCode().value()
                            + ": " + e.getResponseBodyAsString(), e);

        } catch (ResourceAccessException e) {
            // Network/timeout errors — throw exception so Zeebe can retry the job
            throw new GenesysRestException("Genesys API connection error: " + e.getMessage(), e);
        }
    }

    /**
     * Resolves the URL: if it starts with "/", prepend the Genesys base URL.
     * Also appends query parameters if provided.
     */
    private String resolveUrl(String url, Map<String, Object> queryParameters) {
        String fullUrl = url;
        if (url.startsWith("/")) {
            fullUrl = config.getApiBaseUrl() + url;
        }

        if (queryParameters != null && !queryParameters.isEmpty()) {
            UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(fullUrl);
            queryParameters.forEach((key, value) -> {
                if (value == null) return;

                if (value instanceof java.util.Collection<?>) {
                    builder.queryParam(key, (java.util.Collection<?>) value);
                } else if (value instanceof Map || value.getClass().isArray()) {
                    try {
                        String jsonValue = objectMapper.writeValueAsString(value);
                        builder.queryParam(key, jsonValue);
                    } catch (Exception e) {
                        builder.queryParam(key, value.toString());
                    }
                } else {
                    builder.queryParam(key, value.toString());
                }
            });
            fullUrl = builder.build().encode().toUriString();
        }

        return fullUrl;
    }

    /**
     * Builds HTTP headers with auto-injected Bearer token.
     * Custom headers from the BPMN are merged in (but won't override Authorization).
     */
    private HttpHeaders buildHeaders(Map<String, String> customHeaders) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(tokenProvider.getAccessToken());

        if (customHeaders != null) {
            customHeaders.forEach((key, value) -> {
                // Don't let BPMN override the Authorization header
                if (!HttpHeaders.AUTHORIZATION.equalsIgnoreCase(key)) {
                    headers.set(key, value);
                }
            });
        }

        // Default to JSON if no Content-Type was set by custom headers
        if (!headers.containsKey(HttpHeaders.CONTENT_TYPE)) {
            headers.setContentType(MediaType.APPLICATION_JSON);
        }

        return headers;
    }

    /**
     * Returns a RestTemplate with per-request timeout overrides,
     * or the default one if no overrides are provided.
     * RestTemplate instances are cached to prevent resource/connection pool leakage.
     */
    private RestTemplate getRestTemplate(Integer connectTimeoutOverride, Integer readTimeoutOverride) {
        if (connectTimeoutOverride == null && readTimeoutOverride == null) {
            return defaultRestTemplate;
        }

        int connectTimeout = connectTimeoutOverride != null
                ? connectTimeoutOverride : config.getConnectTimeoutSeconds();
        int readTimeout = readTimeoutOverride != null
                ? readTimeoutOverride : config.getReadTimeoutSeconds();

        String cacheKey = connectTimeout + "_" + readTimeout;
        return restTemplateCache.computeIfAbsent(cacheKey, key -> {
            log.info("Creating custom RestTemplate for overrides: connectTimeout={}s, readTimeout={}s",
                    connectTimeout, readTimeout);
            return restTemplateBuilder
                    .requestFactory(org.springframework.http.client.JdkClientHttpRequestFactory.class)
                    .setConnectTimeout(Duration.ofSeconds(connectTimeout))
                    .setReadTimeout(Duration.ofSeconds(readTimeout))
                    .build();
        });
    }

    private GenesysRestResponse buildResponse(ResponseEntity<String> response) {
        Map<String, String> responseHeaders = new LinkedHashMap<>();
        response.getHeaders().forEach((key, values) -> {
            if (values != null && !values.isEmpty()) {
                responseHeaders.put(key, values.get(0));
            }
        });

        Object parsedBody = parseResponseBody(response.getBody());

        return GenesysRestResponse.builder()
                .status(response.getStatusCode().value())
                .headers(responseHeaders)
                .body(parsedBody)
                .build();
    }

    private GenesysRestResponse buildErrorResponse(HttpStatusCodeException e) {
        Map<String, String> responseHeaders = new LinkedHashMap<>();
        HttpHeaders errorHeaders = e.getResponseHeaders();
        if (errorHeaders != null) {
            errorHeaders.forEach((key, values) -> {
                if (values != null && !values.isEmpty()) {
                    responseHeaders.put(key, values.get(0));
                }
            });
        }

        Object parsedBody = parseResponseBody(e.getResponseBodyAsString());

        return GenesysRestResponse.builder()
                .status(e.getStatusCode().value())
                .headers(responseHeaders)
                .body(parsedBody)
                .build();
    }

    /**
     * Attempts to parse the response body as JSON (Map or List).
     * Falls back to raw String if parsing fails.
     */
    private Object parseResponseBody(String body) {
        if (body == null || body.isBlank()) {
            return null;
        }
        try {
            return objectMapper.readValue(body, Object.class);
        } catch (JsonProcessingException e) {
            log.debug("Response body is not JSON, returning as raw string");
            return body;
        }
    }

    /**
     * Extracts custom headers (non-standard) from existing headers.
     */
    private Map<String, String> extractCustomHeaders(HttpHeaders headers) {
        Map<String, String> custom = new LinkedHashMap<>();
        headers.forEach((key, values) -> {
            if (!HttpHeaders.AUTHORIZATION.equalsIgnoreCase(key)
                    && !HttpHeaders.CONTENT_TYPE.equalsIgnoreCase(key)
                    && values != null && !values.isEmpty()) {
                custom.put(key, values.get(0));
            }
        });
        return custom;
    }
}
