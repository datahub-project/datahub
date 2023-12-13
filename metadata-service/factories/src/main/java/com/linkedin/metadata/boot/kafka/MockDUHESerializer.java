package com.linkedin.metadata.boot.kafka;

import static com.linkedin.gms.factory.kafka.schemaregistry.DUHESchemaRegistryFactory.DUHE_SCHEMA_REGISTRY_TOPIC_KEY;

import com.linkedin.metadata.EventUtils;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** Used for early bootstrap to avoid contact with not yet existing schema registry */
@Slf4j
public class MockDUHESerializer extends KafkaAvroSerializer {

  private static final String DATAHUB_UPGRADE_HISTORY_EVENT_SUBJECT_SUFFIX = "-value";

  private String topicName;

  public MockDUHESerializer() {
    this.schemaRegistry = buildMockSchemaRegistryClient();
  }

  public MockDUHESerializer(SchemaRegistryClient client) {
    super(client);
    this.schemaRegistry = buildMockSchemaRegistryClient();
  }

  public MockDUHESerializer(SchemaRegistryClient client, Map<String, ?> props) {
    super(client, props);
    this.schemaRegistry = buildMockSchemaRegistryClient();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    topicName = configs.get(DUHE_SCHEMA_REGISTRY_TOPIC_KEY).toString();
  }

  private MockSchemaRegistryClient buildMockSchemaRegistryClient() {
    MockSchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    try {
      schemaRegistry.register(
          topicToSubjectName(topicName), new AvroSchema(EventUtils.ORIGINAL_DUHE_AVRO_SCHEMA));
      return schemaRegistry;
    } catch (IOException | RestClientException e) {
      throw new RuntimeException(e);
    }
  }

  public static String topicToSubjectName(String topicName) {
    return topicName + DATAHUB_UPGRADE_HISTORY_EVENT_SUBJECT_SUFFIX;
  }
}
