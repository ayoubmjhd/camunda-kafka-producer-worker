package com.camunda.kafka.application.usecase;

import com.camunda.kafka.application.port.outbound.GenesysRestOutboundPort;
import com.camunda.kafka.domain.port.inbound.usecase.genesys.GenesysRestCallUseCase;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestRequest;
import com.camunda.kafka.domain.port.outbound.genesys.GenesysRestResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/**
 * Concrete implementation of the Genesys-REST use-case.
 * It simply forwards the request to the outbound connector.
 */
@Service
@RequiredArgsConstructor
public class GenesysRestCallUseCaseImpl implements GenesysRestCallUseCase {

    private final GenesysRestOutboundPort genesysRestOutboundPort;

    @Override
    public GenesysRestResponse execute(GenesysRestRequest request) {
        return genesysRestOutboundPort.call(request);
    }
}
