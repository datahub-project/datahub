package com.linkedin.metadata.boot.kafka;

import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY;
import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.MCP_SCHEMA_REGISTRY_TOPIC_KEY;
import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX;
import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.SYSTEM_UPDATE_TOPIC_KEY_PREFIX;
import static com.linkedin.metadata.boot.kafka.MockSystemUpdateSerializer.topicToSubjectName;

import com.linkedin.metadata.EventUtils;
import com.linkedin.util.Pair;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Used for early bootstrap to avoid contact with not yet existing schema registry. Supports MCP and
 * MCL topics for system-update.
 */
@Slf4j
public class MockSystemUpdateDeserializer extends KafkaAvroDeserializer {

  private static final Map<String, AvroSchema> AVRO_SCHEMA_MAP =
      Map.of(
          MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY,
          new AvroSchema(EventUtils.RENAMED_MCL_AVRO_SCHEMA),
          MCP_SCHEMA_REGISTRY_TOPIC_KEY,
          new AvroSchema(EventUtils.RENAMED_MCP_AVRO_SCHEMA));

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    Map<String, Pair<AvroSchema, Integer>> topicNameToAvroSchemaMap =
        configs.entrySet().stream()
            .filter(
                e ->
                    e.getKey().startsWith(SYSTEM_UPDATE_TOPIC_KEY_PREFIX)
                        && !e.getKey().endsWith(SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX)
                        && e.getValue() instanceof String)
            .map(
                e -> {
                  String topicName = (String) e.getValue();
                  String topicKey = e.getKey();
                  Integer id =
                      Integer.valueOf(
                          (String) configs.get(topicKey + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX));
                  AvroSchema schema = AVRO_SCHEMA_MAP.get(topicKey);
                  if (schema == null) {
                    throw new IllegalStateException("No schema found for topic key: " + topicKey);
                  }
                  return Pair.of(topicName, Pair.of(schema, id));
                })
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    this.schemaRegistry = buildMockSchemaRegistryClient(topicNameToAvroSchemaMap);
  }

  @com.google.common.annotations.VisibleForTesting
  public MockSchemaRegistryClient getSchemaRegistryClient() {
    return (MockSchemaRegistryClient) schemaRegistry;
  }

  private MockSchemaRegistryClient buildMockSchemaRegistryClient(
      Map<String, Pair<AvroSchema, Integer>> topicNameToAvroSchemaMap) {
    MockSchemaRegistryClient schemaRegistry =
        new CustomMockSchemaRegistryClient(topicNameToAvroSchemaMap);
    try {
      for (Map.Entry<String, Pair<AvroSchema, Integer>> entry :
          topicNameToAvroSchemaMap.entrySet()) {
        schemaRegistry.register(
            topicToSubjectName(entry.getKey()),
            entry.getValue().getFirst(),
            1,
            entry.getValue().getSecond());
      }
      return schemaRegistry;
    } catch (IOException | RestClientException e) {
      throw new RuntimeException(e);
    }
  }

  private static class CustomMockSchemaRegistryClient extends MockSchemaRegistryClient {
    private final Map<String, Pair<AvroSchema, Integer>> topicNameToAvroSchemaMap;

    public CustomMockSchemaRegistryClient(
        Map<String, Pair<AvroSchema, Integer>> topicNameToAvroSchemaMap) {
      this.topicNameToAvroSchemaMap = topicNameToAvroSchemaMap;
    }

    @Override
    public synchronized int register(String subject, ParsedSchema schema, int version, int id)
        throws IOException, RestClientException {
      String topicName =
          subject.endsWith("-value") ? subject.substring(0, subject.length() - 6) : subject;
      Integer configuredId =
          topicNameToAvroSchemaMap.entrySet().stream()
              .filter(entry -> entry.getKey().equals(topicName))
              .findFirst()
              .map(entry -> entry.getValue().getSecond())
              .orElse(id);
      return super.register(subject, schema, version, configuredId);
    }

    @Override
    public synchronized String getCompatibility(String subject)
        throws IOException, RestClientException {
      return "NONE";
    }

    @Override
    public synchronized boolean testCompatibility(String subject, ParsedSchema newSchema)
        throws IOException, RestClientException {
      return true;
    }
  }
}
