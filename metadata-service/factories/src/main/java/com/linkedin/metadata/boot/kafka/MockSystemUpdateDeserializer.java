package com.linkedin.metadata.boot.kafka;

import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.DUHE_SCHEMA_REGISTRY_TOPIC_KEY;
import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX;
import static com.linkedin.metadata.boot.kafka.MockSystemUpdateSerializer.topicToSubjectName;
import static io.datahubproject.openapi.schema.registry.Constants.FIXED_SCHEMA_VERSION;

import com.linkedin.metadata.EventUtils;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Used for early bootstrap to avoid contact with not yet existing schema registry Only supports the
 * DUHE topic
 */
@Slf4j
public class MockSystemUpdateDeserializer extends KafkaAvroDeserializer {

  private String topicName;
  private Integer schemaId;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    topicName = configs.get(DUHE_SCHEMA_REGISTRY_TOPIC_KEY).toString();
    schemaId =
        Integer.valueOf(
            configs
                .get(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX)
                .toString());
    this.schemaRegistry = buildMockSchemaRegistryClient();
  }

  private MockSchemaRegistryClient buildMockSchemaRegistryClient() {
    MockSchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient2(schemaId);
    try {
      schemaRegistry.register(
          topicToSubjectName(topicName),
          new AvroSchema(EventUtils.ORIGINAL_DUHE_AVRO_SCHEMA),
          FIXED_SCHEMA_VERSION,
          schemaId);
      return schemaRegistry;
    } catch (IOException | RestClientException e) {
      throw new RuntimeException(e);
    }
  }

  public static class MockSchemaRegistryClient2 extends MockSchemaRegistryClient {
    private final int schemaId;

    public MockSchemaRegistryClient2(int schemaId) {
      this.schemaId = schemaId;
    }

    /**
     * Previously used topics can have schema ids > 1 which fully match however we are replacing
     * that registry so force schema id to 1
     */
    @Override
    public synchronized ParsedSchema getSchemaById(int id) throws IOException, RestClientException {
      return super.getSchemaById(schemaId);
    }
  }
}
