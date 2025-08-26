package com.linkedin.metadata.boot.kafka;

import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.DUHE_SCHEMA_REGISTRY_TOPIC_KEY;
import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY;
import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.MCP_SCHEMA_REGISTRY_TOPIC_KEY;
import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX;
import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.SYSTEM_UPDATE_TOPIC_KEY_PREFIX;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.registry.SchemaRegistryServiceImpl;
import com.linkedin.util.Pair;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.util.HashMap;
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

  // Mapping from schema registry topic keys to schema names
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
                  String topicName = (String) e.getValue();
                  String topicKey = e.getKey();
                  Integer id =
                      Integer.valueOf(
                          (String) configs.get(topicKey + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX));

                  // Get the schema from AVRO_SCHEMA_MAP based on the topic key
                  AvroSchema schema = AVRO_SCHEMA_MAP.get(topicKey);
                  if (schema == null) {
                    throw new IllegalStateException("No schema found for topic key: " + topicKey);
                  }

                  return Pair.of(topicName, Pair.of(schema, id));
                })
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    this.schemaRegistry = buildMockSchemaRegistryClient();
  }

  private MockSchemaRegistryClient buildMockSchemaRegistryClient() {
    MockSchemaRegistryClient schemaRegistry =
        new CustomMockSchemaRegistryClient(topicNameToAvroSchemaMap);

    if (topicNameToAvroSchemaMap != null) {
      topicNameToAvroSchemaMap.forEach(
          (topicName, schemaAndId) -> {
            try {
              // Get the schema name for this topic to determine the version
              String schemaName = getSchemaNameForTopic(topicName);

              // Use SchemaRegistryServiceImpl static method to get the proper version
              int version =
                  SchemaRegistryServiceImpl.getVersionForSchemaId(
                      schemaName, schemaAndId.getSecond());

              schemaRegistry.register(
                  topicToSubjectName(topicName),
                  schemaAndId.getFirst(),
                  version,
                  schemaAndId.getSecond());
            } catch (IOException | RestClientException e) {
              throw new RuntimeException(e);
            }
          });
    }

    return schemaRegistry;
  }

  /** Get the schema name for a topic by looking up the schema in AVRO_SCHEMA_MAP */
  private String getSchemaNameForTopic(String topicName) {
    // Find the schema registry key that maps to this topic's schema
    for (Map.Entry<String, Pair<AvroSchema, Integer>> entry : topicNameToAvroSchemaMap.entrySet()) {
      if (entry.getKey().equals(topicName)) {
        AvroSchema schema = entry.getValue().getFirst();
        // Find the key in AVRO_SCHEMA_MAP that has this schema
        for (Map.Entry<String, AvroSchema> schemaEntry : AVRO_SCHEMA_MAP.entrySet()) {
          if (schemaEntry.getValue().equals(schema)) {
            return SCHEMA_REGISTRY_KEY_TO_SCHEMA_NAME_MAP.get(schemaEntry.getKey());
          }
        }
      }
    }
    throw new IllegalStateException("Could not find schema name for topic: " + topicName);
  }

  /**
   * Custom MockSchemaRegistryClient that forces specific schema IDs instead of auto-assigning
   * sequential ones and handles historical schema ID requests for backward compatibility
   */
  private static class CustomMockSchemaRegistryClient extends MockSchemaRegistryClient {
    private final Map<String, Pair<AvroSchema, Integer>> topicNameToAvroSchemaMap;
    private final Map<String, Integer> subjectToIdMap = new HashMap<>();

    public CustomMockSchemaRegistryClient(
        Map<String, Pair<AvroSchema, Integer>> topicNameToAvroSchemaMap) {
      this.topicNameToAvroSchemaMap = topicNameToAvroSchemaMap;
    }

    @Override
    public synchronized int register(String subject, ParsedSchema schema, int version, int id)
        throws IOException, RestClientException {
      // Find the topic name for this subject (remove "-value" suffix)
      String topicName =
          subject.endsWith("-value") ? subject.substring(0, subject.length() - 6) : subject;

      // Look up the configured schema ID for this topic
      Integer configuredId =
          topicNameToAvroSchemaMap.entrySet().stream()
              .filter(entry -> entry.getKey().equals(topicName))
              .findFirst()
              .map(entry -> entry.getValue().getSecond())
              .orElse(id); // Fallback to provided id if not found

      // Store the mapping from subject to configured ID
      subjectToIdMap.put(subject, configuredId);

      // Call parent method with the configured ID
      return super.register(subject, schema, version, configuredId);
    }

    @Override
    public synchronized ParsedSchema getSchemaById(int id) throws IOException, RestClientException {
      // First try to find a subject that maps to this ID
      for (Map.Entry<String, Integer> entry : subjectToIdMap.entrySet()) {
        if (entry.getValue().equals(id)) {
          // If we find a mapping, return the schema for the configured ID
          return super.getSchemaById(entry.getValue());
        }
      }

      // Fallback to parent method
      return super.getSchemaById(id);
    }
  }

  @VisibleForTesting
  public SchemaRegistryClient getSchemaRegistryClient() {
    return schemaRegistry;
  }

  /**
   * Get the topic name for a given schema ID
   *
   * @param schemaId the schema ID to look up
   * @return the topic name, or null if not found
   */
  @VisibleForTesting
  public String getTopicNameForSchemaId(int schemaId) {
    if (topicNameToAvroSchemaMap == null) {
      return null;
    }

    return topicNameToAvroSchemaMap.entrySet().stream()
        .filter(entry -> entry.getValue().getSecond().equals(schemaId))
        .findFirst()
        .map(Map.Entry::getKey)
        .orElse(null);
  }

  /**
   * Get all configured schema IDs for debugging purposes
   *
   * @return map of topic name to schema ID
   */
  @VisibleForTesting
  public Map<String, Integer> getConfiguredSchemaIds() {
    if (topicNameToAvroSchemaMap == null) {
      return Map.of();
    }

    return topicNameToAvroSchemaMap.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getSecond()));
  }

  public static String topicToSubjectName(String topicName) {
    return topicName + DATAHUB_SYSTEM_UPDATE_SUBJECT_SUFFIX;
  }
}
