package com.camunda.kafka.application.port.outbound;

import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestRequest;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestResponse;

/**
 * Outbound port used by the use-case to perform a Genesys REST call.
 * The concrete implementation lives in the adapter.outbound.genesys package.
 */
public interface GenesysRestOutboundPort {

    /**
     * Executes the HTTP request.
     *
     * @param request domain request DTO
     * @return domain response DTO
     */
    GenesysRestResponse call(GenesysRestRequest request);
}
