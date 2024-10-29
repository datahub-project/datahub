package com.linkedin.metadata.boot.kafka;

import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.DUHE_SCHEMA_REGISTRY_TOPIC_KEY;
import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY;
import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.MCP_SCHEMA_REGISTRY_TOPIC_KEY;
import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX;
import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.SYSTEM_UPDATE_TOPIC_KEY_PREFIX;
import static io.datahubproject.openapi.schema.registry.Constants.FIXED_SCHEMA_VERSION;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.EventUtils;
import com.linkedin.util.Pair;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Used for early bootstrap to avoid contact with not yet existing schema registry */
@Slf4j
public class MockSystemUpdateSerializer extends KafkaAvroSerializer {

  private static final String DATAHUB_SYSTEM_UPDATE_SUBJECT_SUFFIX = "-value";

  private static final Map<String, AvroSchema> AVRO_SCHEMA_MAP =
      Map.of(
          DUHE_SCHEMA_REGISTRY_TOPIC_KEY,
          new AvroSchema(EventUtils.ORIGINAL_DUHE_AVRO_SCHEMA),
          MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY,
          new AvroSchema(EventUtils.RENAMED_MCL_AVRO_SCHEMA),
          MCP_SCHEMA_REGISTRY_TOPIC_KEY,
          new AvroSchema(EventUtils.RENAMED_MCP_AVRO_SCHEMA));

  private Map<String, Pair<AvroSchema, Integer>> topicNameToAvroSchemaMap;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    topicNameToAvroSchemaMap =
        configs.entrySet().stream()
            .filter(
                e ->
                    e.getKey().startsWith(SYSTEM_UPDATE_TOPIC_KEY_PREFIX)
                        && !e.getKey().endsWith(SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX)
                        && e.getValue() instanceof String)
            .map(
                e -> {
                  Integer id =
                      Integer.valueOf(
                          (String) configs.get(e.getKey() + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX));
                  return Pair.of(
                      (String) e.getValue(), Pair.of(AVRO_SCHEMA_MAP.get(e.getKey()), id));
                })
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    this.schemaRegistry = buildMockSchemaRegistryClient();
  }

  private MockSchemaRegistryClient buildMockSchemaRegistryClient() {
    MockSchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();

    if (topicNameToAvroSchemaMap != null) {
      topicNameToAvroSchemaMap.forEach(
          (topicName, schemaId) -> {
            try {
              schemaRegistry.register(
                  topicToSubjectName(topicName),
                  schemaId.getFirst(),
                  FIXED_SCHEMA_VERSION,
                  schemaId.getSecond());
            } catch (IOException | RestClientException e) {
              throw new RuntimeException(e);
            }
          });
    }

    return schemaRegistry;
  }

  @VisibleForTesting
  public SchemaRegistryClient getSchemaRegistryClient() {
    return schemaRegistry;
  }

  public static String topicToSubjectName(String topicName) {
    return topicName + DATAHUB_SYSTEM_UPDATE_SUBJECT_SUFFIX;
  }
}
