package com.linkedin.metadata.boot.kafka;

import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.DUHE_SCHEMA_REGISTRY_TOPIC_KEY;
import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX;
import static com.linkedin.metadata.boot.kafka.MockSystemUpdateSerializer.topicToSubjectName;

import com.linkedin.metadata.EventSchemaConstants;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

/**
 * Used for early bootstrap to avoid contact with not yet existing schema registry Only supports the
 * DUHE topic
 */
@Slf4j
public class MockSystemUpdateDeserializer extends KafkaAvroDeserializer {

  private Integer schemaId;
  private Map<String, ?> configs;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    this.configs = configs;
    schemaId =
        Integer.valueOf(
            configs
                .get(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX)
                .toString());
    this.schemaRegistry = buildMockSchemaRegistryClient();
  }

  private MockSchemaRegistryClient buildMockSchemaRegistryClient() {
    MockSchemaRegistryClient schemaRegistry = new CustomMockSchemaRegistryClient(schemaId);
    try {
      // Use the same schema constants as SchemaRegistryServiceImpl
      Schema duheSchema = EventSchemaConstants.DUHE_SCHEMA;

      // Get the topic name from configs for subject name generation
      String topicName = configs.get(DUHE_SCHEMA_REGISTRY_TOPIC_KEY).toString();

      // Register schema
      schemaRegistry.register(
          topicToSubjectName(topicName),
          new AvroSchema(duheSchema),
          1, // Version 1 since each schema ID represents a backwards incompatible version
          schemaId);

      return schemaRegistry;
    } catch (IOException | RestClientException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Custom MockSchemaRegistryClient that ensures the configured schema ID is used and handles
   * historical schema ID requests for backward compatibility
   */
  private static class CustomMockSchemaRegistryClient extends MockSchemaRegistryClient {
    private final int schemaId;

    public CustomMockSchemaRegistryClient(int schemaId) {
      this.schemaId = schemaId;
    }

    @Override
    public synchronized int register(String subject, ParsedSchema schema, int version, int id)
        throws IOException, RestClientException {
      // Force the use of the configured schema ID
      return super.register(subject, schema, version, schemaId);
    }

    @Override
    public synchronized String getCompatibility(String subject)
        throws IOException, RestClientException {
      // Return NONE compatibility (most permissive)
      return "NONE";
    }

    @Override
    public synchronized boolean testCompatibility(String subject, ParsedSchema newSchema)
        throws IOException, RestClientException {
      // With NONE compatibility, always return true
      return true;
    }
  }
}
