package com.camunda.kafka.service;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import com.camunda.kafka.exception.KafkaProducerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.IOException;

/**
 * Service for fetching Avro schemas from Confluent Schema Registry by subject name.
 * Uses {@link CachedSchemaRegistryClient} for internal caching.
 */
@Slf4j
@Service
public class SchemaRegistryService {

    private static final int SCHEMA_CACHE_CAPACITY = 100;

    @Value("${spring.kafka.producer.properties.schema.registry.url:http://localhost:8081}")
    private String schemaRegistryUrl;

    private SchemaRegistryClient registryClient;

    @PostConstruct
    public void init() {
        this.registryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, SCHEMA_CACHE_CAPACITY);
        log.info("SchemaRegistryService initialized with URL: {}", schemaRegistryUrl);
    }

    /**
     * Fetch the latest Avro schema for the given subject.
     *
     * @param subject the Schema Registry subject name (e.g., "orders-value")
     * @return the parsed Avro Schema
     * @throws KafkaProducerException if the schema cannot be fetched or parsed
     */
    public Schema getSchema(String subject) {
        try {
            log.debug("Fetching latest schema for subject: {}", subject);
            SchemaMetadata metadata = registryClient.getLatestSchemaMetadata(subject);
            Schema schema = new Schema.Parser().parse(metadata.getSchema());
            log.debug("Schema fetched successfully for subject '{}', version {}, id {}",
                    subject, metadata.getVersion(), metadata.getId());
            return schema;
        } catch (RestClientException e) {
            throw new KafkaProducerException(
                    "Failed to fetch schema for subject '" + subject
                            + "' from Schema Registry: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new KafkaProducerException(
                    "I/O error while fetching schema for subject '" + subject + "': " + e.getMessage(), e);
        }
    }

    /**
     * Fetch a specific version of the Avro schema for the given subject.
     *
     * @param subject the Schema Registry subject name
     * @param version the schema version to fetch
     * @return the parsed Avro Schema
     * @throws KafkaProducerException if the schema cannot be fetched or parsed
     */
    public Schema getSchema(String subject, int version) {
        try {
            log.debug("Fetching schema for subject: {}, version: {}", subject, version);
            SchemaMetadata metadata = registryClient.getSchemaMetadata(subject, version);
            return new Schema.Parser().parse(metadata.getSchema());
        } catch (RestClientException e) {
            throw new KafkaProducerException(
                    "Failed to fetch schema for subject '" + subject
                            + "' version " + version + ": " + e.getMessage(), e);
        } catch (IOException e) {
            throw new KafkaProducerException(
                    "I/O error while fetching schema for subject '" + subject + "': " + e.getMessage(), e);
        }
    }
}
