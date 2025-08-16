package com.linkedin.metadata.boot.kafka;

import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.DUHE_SCHEMA_REGISTRY_TOPIC_KEY;
import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY;
import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.MCP_SCHEMA_REGISTRY_TOPIC_KEY;
import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX;
import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.SYSTEM_UPDATE_TOPIC_KEY_PREFIX;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.EventSchemaConstants;
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
          new AvroSchema(EventUtils.RENAMED_DUHE_AVRO_SCHEMA),
          MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY,
          new AvroSchema(EventUtils.RENAMED_MCL_AVRO_SCHEMA),
          MCP_SCHEMA_REGISTRY_TOPIC_KEY,
          new AvroSchema(EventUtils.RENAMED_MCP_AVRO_SCHEMA));

  // Mapping from schema registry topic keys to schema names for version lookup
  private static final Map<String, String> SCHEMA_REGISTRY_KEY_TO_SCHEMA_NAME_MAP =
      Map.of(
          DUHE_SCHEMA_REGISTRY_TOPIC_KEY,
          EventUtils.DATAHUB_UPGRADE_HISTORY_EVENT_SCHEMA_NAME,
          MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY,
          EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME,
          MCP_SCHEMA_REGISTRY_TOPIC_KEY,
          EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);

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
              // Find the schema registry key that maps to this topic's schema
              String schemaRegistryKey = findSchemaRegistryKeyForTopic(topicName);
              String schemaName = SCHEMA_REGISTRY_KEY_TO_SCHEMA_NAME_MAP.get(schemaRegistryKey);
              int latestVersion = EventSchemaConstants.getLatestSchemaVersion(schemaName);

              schemaRegistry.register(
                  topicToSubjectName(topicName),
                  schemaId.getFirst(),
                  latestVersion,
                  schemaId.getSecond());
            } catch (IOException | RestClientException e) {
              throw new RuntimeException(e);
            }
          });
    }

    return schemaRegistry;
  }

  /** Finds the schema registry key that corresponds to a given topic's schema */
  private String findSchemaRegistryKeyForTopic(String topicName) {
    // Find which schema registry key this topic is using by looking up the schema in
    // AVRO_SCHEMA_MAP
    return topicNameToAvroSchemaMap.entrySet().stream()
        .filter(entry -> entry.getKey().equals(topicName))
        .findFirst()
        .map(
            entry -> {
              AvroSchema schema = entry.getValue().getFirst();
              // Find the key in AVRO_SCHEMA_MAP that has this schema
              return AVRO_SCHEMA_MAP.entrySet().stream()
                  .filter(schemaEntry -> schemaEntry.getValue().equals(schema))
                  .findFirst()
                  .map(Map.Entry::getKey)
                  .orElseThrow(
                      () ->
                          new IllegalStateException(
                              "Could not find schema registry key for schema: "
                                  + schema.toString()));
            })
        .orElseThrow(
            () ->
                new IllegalStateException("Could not find schema mapping for topic: " + topicName));
  }

  @VisibleForTesting
  public SchemaRegistryClient getSchemaRegistryClient() {
    return schemaRegistry;
  }

  public static String topicToSubjectName(String topicName) {
    return topicName + DATAHUB_SYSTEM_UPDATE_SUBJECT_SUFFIX;
  }
}
