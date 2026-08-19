package com.camunda.kafka.domain.port.inbound.usecase.genesys;

import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestRequest;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestResponse;

/**
 * Inbound use-case that the Camunda worker will invoke.
 * It abstracts the actual HTTP call behind a clean contract.
 */
public interface GenesysRestCallUseCase {

    /**
     * Executes a Genesys REST request.
     *
     * @param request the domain request DTO
     * @return the domain response DTO
     */
    GenesysRestResponse execute(GenesysRestRequest request);
}
