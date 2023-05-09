package com.linkedin.gms.factory.kafka.schemaregistry;

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

    private static final String DATAHUB_UPGRADE_HISTORY_EVENT_SUBJECT = "DataHubUpgradeHistory_v1-value";

    public MockDUHESerializer() {
        buildMockSchemaRegistryClient();
    }

    public MockDUHESerializer(SchemaRegistryClient client) {
        buildMockSchemaRegistryClient();
    }

    public MockDUHESerializer(SchemaRegistryClient client, Map<String, ?> props) {
        super(client, props);
        buildMockSchemaRegistryClient();
    }

    private void buildMockSchemaRegistryClient() {
        this.schemaRegistry = new MockSchemaRegistryClient();
        try {
            this.schemaRegistry.register(DATAHUB_UPGRADE_HISTORY_EVENT_SUBJECT,
                    new AvroSchema(EventUtils.ORIGINAL_DUHE_AVRO_SCHEMA));
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
    }
}
