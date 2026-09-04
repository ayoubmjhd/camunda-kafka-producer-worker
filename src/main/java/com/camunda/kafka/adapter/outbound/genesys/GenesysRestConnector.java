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
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Concrete implementation of the outbound port that uses Spring {@link RestClient}
 * to perform OAuth2-protected HTTP calls to Genesys Cloud.
 *
 * <p>Handles:
 * <ul>
 *   <li>OAuth2 Bearer token injection (auto-refreshed via interceptor)</li>
 *   <li>URL resolution (relative paths get the base URL prepended)</li>
 *   <li>Query parameter handling (lists, objects, primitives)</li>
 *   <li>Throws specific exceptions for 408, 429, and 5xx so Zeebe can handle retries</li>
 *   <li>JSON response parsing</li>
 * </ul>
 */
@Slf4j
@Component
public class GenesysRestConnector implements GenesysRestOutboundPort {

    private final RestClient defaultRestClient;
    private final GenesysProperties config;
    private final GenesysTokenProvider tokenProvider;
    private final ObjectMapper objectMapper;

    public GenesysRestConnector(RestClient genesysApiRestClient,
                                GenesysProperties config,
                                GenesysTokenProvider tokenProvider,
                                ObjectMapper objectMapper) {
        this.defaultRestClient = genesysApiRestClient;
        this.config = config;
        this.tokenProvider = tokenProvider;
        this.objectMapper = objectMapper;
    }

    @Override
    public GenesysRestResponse call(GenesysRestRequest request) {
        // 1. Resolve URL
        String resolvedUrl = resolveUrl(request.getUrl(), request.getQueryParameters());

        // 2. Build custom headers (Bearer token is injected by the RestClient interceptor)
        Map<String, String> customHeaders = buildCustomHeaders(request.getHeaders());

        // 3. Process the request body (convert Map to MultiValueMap for form/multipart requests)
        Object requestBody = processRequestBody(request.getBody(), request.getHeaders());

        HttpMethod httpMethod = HttpMethod.valueOf(request.getMethod().toUpperCase());

        // 4. Execute request (letting Zeebe handle retries for failures)
        return executeRequest(defaultRestClient, resolvedUrl, httpMethod, customHeaders, requestBody);
    }

    private GenesysRestResponse executeRequest(
            RestClient restClient, String url, HttpMethod method,
            Map<String, String> customHeaders, Object body) {

        try {
            log.debug("Genesys API call: {} {}", method, url);
            ResponseEntity<String> response = doExchange(restClient, url, method, customHeaders, body);
            return buildResponse(response);

        } catch (HttpClientErrorException e) {
            int statusCode = e.getStatusCode().value();

            // 401: token might be expired — invalidate, refresh, and retry EXACTLY ONCE
            if (statusCode == 401) {
                log.warn("Genesys API returned 401, invalidating token and retrying once...");
                tokenProvider.invalidateToken();

                try {
                    // Interceptor will inject the fresh token on the retry call
                    ResponseEntity<String> retryResponse =
                            doExchange(restClient, url, method, customHeaders, body);
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
     * Performs the actual HTTP exchange using the RestClient fluent API.
     */
    private ResponseEntity<String> doExchange(
            RestClient restClient, String url, HttpMethod method,
            Map<String, String> customHeaders, Object body) {

        RestClient.RequestBodySpec requestSpec = restClient.method(method)
                .uri(java.net.URI.create(url))
                .headers(httpHeaders -> {
                    // Apply custom headers (Authorization is handled by the interceptor)
                    if (customHeaders != null) {
                        customHeaders.forEach(httpHeaders::set);
                    }
                    // Default to JSON if no Content-Type was set by custom headers
                    if (!httpHeaders.containsKey(HttpHeaders.CONTENT_TYPE)) {
                        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
                    }
                });

        if (body != null) {
            requestSpec.body(body);
        }

        return requestSpec.retrieve().toEntity(String.class);
    }

    /**
     * Processes the request body — converts Map to MultiValueMap for form/multipart requests.
     */
    private Object processRequestBody(Object requestBody, Map<String, String> headers) {
        if (requestBody instanceof Map && headers != null) {
            String contentTypeHeader = headers.entrySet().stream()
                    .filter(e -> HttpHeaders.CONTENT_TYPE.equalsIgnoreCase(e.getKey()))
                    .map(Map.Entry::getValue)
                    .findFirst()
                    .orElse(null);

            if (contentTypeHeader != null) {
                MediaType contentType = MediaType.parseMediaType(contentTypeHeader);
                if (MediaType.APPLICATION_FORM_URLENCODED.includes(contentType)
                        || MediaType.MULTIPART_FORM_DATA.includes(contentType)) {
                    MultiValueMap<String, Object> formParams = new LinkedMultiValueMap<>();
                    ((Map<?, ?>) requestBody).forEach((k, v) -> {
                        if (v != null) {
                            formParams.add(k.toString(), v);
                        }
                    });
                    return formParams;
                }
            }
        }
        return requestBody;
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
                    } catch (JsonProcessingException e) {
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
     * Extracts custom headers from the request, filtering out Authorization
     * (which is injected by the RestClient interceptor).
     */
    private Map<String, String> buildCustomHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return null;
        }
        Map<String, String> filtered = new LinkedHashMap<>();
        headers.forEach((key, value) -> {
            if (!HttpHeaders.AUTHORIZATION.equalsIgnoreCase(key)) {
                filtered.put(key, value);
            }
        });
        return filtered.isEmpty() ? null : filtered;
    }


    private GenesysRestResponse buildResponse(ResponseEntity<String> response) {
        Map<String, String> responseHeaders = extractHeaders(response.getHeaders());
        Object parsedBody = parseResponseBody(response.getBody());

        return GenesysRestResponse.builder()
                .status(response.getStatusCode().value())
                .headers(responseHeaders)
                .body(parsedBody)
                .build();
    }

    private GenesysRestResponse buildErrorResponse(HttpStatusCodeException e) {
        Map<String, String> responseHeaders = extractHeaders(e.getResponseHeaders());
        Object parsedBody = parseResponseBody(e.getResponseBodyAsString());

        return GenesysRestResponse.builder()
                .status(e.getStatusCode().value())
                .headers(responseHeaders)
                .body(parsedBody)
                .build();
    }

    private Map<String, String> extractHeaders(HttpHeaders httpHeaders) {
        Map<String, String> responseHeaders = new LinkedHashMap<>();
        if (httpHeaders != null) {
            httpHeaders.forEach((key, values) -> {
                if (values != null && !values.isEmpty()) {
                    responseHeaders.put(key, values.get(0));
                }
            });
        }
        return responseHeaders;
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
}
