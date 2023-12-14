package com.linkedin.metadata.boot.kafka;

import static com.linkedin.gms.factory.kafka.schemaregistry.DUHESchemaRegistryFactory.DUHE_SCHEMA_REGISTRY_TOPIC_KEY;
import static com.linkedin.metadata.boot.kafka.MockDUHESerializer.topicToSubjectName;

import com.linkedin.metadata.EventUtils;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** Used for early bootstrap to avoid contact with not yet existing schema registry */
@Slf4j
public class MockDUHEDeserializer extends KafkaAvroDeserializer {

  private String topicName;

  public MockDUHEDeserializer() {
    this.schemaRegistry = buildMockSchemaRegistryClient();
  }

  public MockDUHEDeserializer(SchemaRegistryClient client) {
    super(client);
    this.schemaRegistry = buildMockSchemaRegistryClient();
  }

  public MockDUHEDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
    super(client, props);
    this.schemaRegistry = buildMockSchemaRegistryClient();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    topicName = configs.get(DUHE_SCHEMA_REGISTRY_TOPIC_KEY).toString();
  }

  private MockSchemaRegistryClient buildMockSchemaRegistryClient() {
    MockSchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient2();
    try {
      schemaRegistry.register(
          topicToSubjectName(topicName), new AvroSchema(EventUtils.ORIGINAL_DUHE_AVRO_SCHEMA));
      return schemaRegistry;
    } catch (IOException | RestClientException e) {
      throw new RuntimeException(e);
    }
  }

  public static class MockSchemaRegistryClient2 extends MockSchemaRegistryClient {
    /**
     * Previously used topics can have schema ids > 1 which fully match however we are replacing
     * that registry so force schema id to 1
     */
    @Override
    public synchronized ParsedSchema getSchemaById(int id) throws IOException, RestClientException {
      return super.getSchemaById(1);
    }
  }
}
