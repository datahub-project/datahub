package com.linkedin.metadata.boot.kafka;

import com.linkedin.metadata.EventUtils;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;

/**
 * Used for early bootstrap to avoid contact with not yet existing schema registry
 */
@Slf4j
public class MockDUHESerializer extends KafkaAvroSerializer {

    static final String DATAHUB_UPGRADE_HISTORY_EVENT_SUBJECT = "DataHubUpgradeHistory_v1-value";

    public MockDUHESerializer() {
        this.schemaRegistry = buildMockSchemaRegistryClient();
    }

    public MockDUHESerializer(SchemaRegistryClient client) {
        this.schemaRegistry = buildMockSchemaRegistryClient();
    }

    public MockDUHESerializer(SchemaRegistryClient client, Map<String, ?> props) {
        super(client, props);
        this.schemaRegistry = buildMockSchemaRegistryClient();
    }

    private static MockSchemaRegistryClient buildMockSchemaRegistryClient() {
        MockSchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        try {
            schemaRegistry.register(DATAHUB_UPGRADE_HISTORY_EVENT_SUBJECT,
                    new AvroSchema(EventUtils.ORIGINAL_DUHE_AVRO_SCHEMA));
            return schemaRegistry;
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
    }
}
