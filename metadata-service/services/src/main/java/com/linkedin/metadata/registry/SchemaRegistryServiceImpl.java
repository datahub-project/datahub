package com.linkedin.metadata.registry;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.EventSchemaConstants;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.SchemaIdOrdinal;
import com.linkedin.mxe.TopicConvention;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;

/**
 * Schema Registry Service Implementation that maintains backward compatibility with existing schema
 * IDs.
 *
 * <p>Schema ID mapping is defined in {@link SchemaIdOrdinal} enum to maintain consistency and
 * prevent accidental changes to these critical values.
 */
public class SchemaRegistryServiceImpl implements SchemaRegistryService {

  private final Map<String, Map<Integer, Schema>> _versionedSchemaMap;
  private final Map<String, Map<Integer, Integer>> _versionToSchemaIdMap;
  private final Map<Integer, String> _schemaIdToTopicMap;
  private final Map<String, String> _schemaCompatibilityMap;

  public SchemaRegistryServiceImpl(final TopicConvention convention) {
    this._versionedSchemaMap = new HashMap<>();
    this._versionToSchemaIdMap = new HashMap<>();
    this._schemaIdToTopicMap = new HashMap<>();
    this._schemaCompatibilityMap = new HashMap<>();

    // Initialize all schemas using centralized constants and topic convention
    initializeAllSchemas(convention);
  }

  private void initializeAllSchemas(final TopicConvention convention) {
    // Create topic-to-schema mapping using TopicConvention
    Map<String, String> topicToSchemaNameMap = createTopicToSchemaNameMap(convention);

    // Initialize schemas for each topic
    for (Map.Entry<String, String> entry : topicToSchemaNameMap.entrySet()) {
      String topicName = entry.getKey();
      String schemaName = entry.getValue();

      Map<Integer, Schema> topicSchemas = new HashMap<>();

      // Get all schema IDs for this schema name
      Optional<List<Integer>> schemaIdsOpt =
          EventSchemaConstants.getSchemaIdsForSchemaName(schemaName);
      if (schemaIdsOpt.isEmpty()) {
        continue;
      }

      List<Integer> schemaIds = schemaIdsOpt.get();

      // Add schemas for each schema ID
      // Each schema ID represents a different version of the schema (backwards incompatible)
      for (int i = 0; i < schemaIds.size(); i++) {
        int version = calculateVersionFromIndex(i);
        int schemaId = schemaIds.get(i);

        Schema schema = getSchemaForSchemaIdFromConstants(schemaId);
        if (schema != null) {
          topicSchemas.put(version, schema);

          // Build version to schema ID mapping
          if (!_versionToSchemaIdMap.containsKey(topicName)) {
            _versionToSchemaIdMap.put(topicName, new HashMap<>());
          }
          _versionToSchemaIdMap.get(topicName).put(version, schemaId);

          // Build schema ID to topic mapping
          _schemaIdToTopicMap.put(schemaId, topicName);
        }
      }

      if (!topicSchemas.isEmpty()) {
        _versionedSchemaMap.put(topicName, topicSchemas);

        // Set compatibility based on number of versions:
        // - Single version schemas: BACKWARD (can evolve backward-compatibly)
        // - Multi-version schemas: NONE (breaking changes between versions)
        String compatibility =
            (schemaIds.size() == 1)
                ? EventSchemaConstants.SCHEMA_COMPATIBILITY_BACKWARD
                : EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE;
        _schemaCompatibilityMap.put(topicName, compatibility);
      }
    }
  }

  /** Calculate version number from array index (0-based to 1-based) */
  private static int calculateVersionFromIndex(int index) {
    return index + 1; // Version 1, 2, 3, etc.
  }

  private Map<String, String> createTopicToSchemaNameMap(TopicConvention convention) {
    Map<String, String> topicToSchemaNameMap = new HashMap<>();

    // Map configured topic names to their corresponding schema names
    topicToSchemaNameMap.put(
        convention.getMetadataChangeProposalTopicName(),
        EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);
    topicToSchemaNameMap.put(
        convention.getFailedMetadataChangeProposalTopicName(),
        EventUtils.FAILED_METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);
    topicToSchemaNameMap.put(
        convention.getMetadataChangeLogVersionedTopicName(),
        EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME);
    topicToSchemaNameMap.put(
        convention.getMetadataChangeLogTimeseriesTopicName(),
        EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME);
    topicToSchemaNameMap.put(
        convention.getPlatformEventTopicName(), EventUtils.PLATFORM_EVENT_SCHEMA_NAME);
    topicToSchemaNameMap.put(
        convention.getMetadataChangeEventTopicName(), EventUtils.METADATA_CHANGE_EVENT_SCHEMA_NAME);
    topicToSchemaNameMap.put(
        convention.getFailedMetadataChangeEventTopicName(),
        EventUtils.FAILED_METADATA_CHANGE_EVENT_SCHEMA_NAME);
    topicToSchemaNameMap.put(
        convention.getMetadataAuditEventTopicName(), EventUtils.METADATA_AUDIT_EVENT_SCHEMA_NAME);
    topicToSchemaNameMap.put(
        convention.getDataHubUpgradeHistoryTopicName(),
        EventUtils.DATAHUB_UPGRADE_HISTORY_EVENT_SCHEMA_NAME);

    return topicToSchemaNameMap;
  }

  private Schema getSchemaForSchemaIdFromConstants(int schemaId) {
    // Map schema IDs to their corresponding schema constants
    if (schemaId == EventSchemaConstants.MCP_V1_SCHEMA_ID) {
      return EventSchemaConstants.MCP_V1_SCHEMA;
    } else if (schemaId == EventSchemaConstants.MCP_SCHEMA_ID) {
      return EventSchemaConstants.MCP_SCHEMA;
    } else if (schemaId == EventSchemaConstants.FMCP_V1_SCHEMA_ID) {
      return EventSchemaConstants.FMCP_V1_SCHEMA;
    } else if (schemaId == EventSchemaConstants.FMCP_SCHEMA_ID) {
      return EventSchemaConstants.FMCP_SCHEMA;
    } else if (schemaId == EventSchemaConstants.MCL_V1_SCHEMA_ID) {
      return EventSchemaConstants.MCL_V1_SCHEMA;
    } else if (schemaId == EventSchemaConstants.MCL_SCHEMA_ID) {
      return EventSchemaConstants.MCL_SCHEMA;
    } else if (schemaId == EventSchemaConstants.MCL_TIMESERIES_V1_SCHEMA_ID) {
      return EventSchemaConstants.MCL_TIMESERIES_V1_SCHEMA;
    } else if (schemaId == EventSchemaConstants.MCL_TIMESERIES_SCHEMA_ID) {
      return EventSchemaConstants.MCL_TIMESERIES_SCHEMA;
    } else if (schemaId == EventSchemaConstants.PE_SCHEMA_ID) {
      return EventSchemaConstants.PE_SCHEMA;
    } else if (schemaId == EventSchemaConstants.MCE_V1_SCHEMA_ID) {
      return EventSchemaConstants.MCE_V1_SCHEMA;
    } else if (schemaId == EventSchemaConstants.MCE_SCHEMA_ID) {
      return EventSchemaConstants.MCE_SCHEMA;
    } else if (schemaId == EventSchemaConstants.FMCE_V1_SCHEMA_ID) {
      return EventSchemaConstants.FMCE_V1_SCHEMA;
    } else if (schemaId == EventSchemaConstants.FMCE_SCHEMA_ID) {
      return EventSchemaConstants.FMCE_SCHEMA;
    } else if (schemaId == EventSchemaConstants.MAE_V1_SCHEMA_ID) {
      return EventSchemaConstants.MAE_V1_SCHEMA;
    } else if (schemaId == EventSchemaConstants.MAE_SCHEMA_ID) {
      return EventSchemaConstants.MAE_SCHEMA;
    } else if (schemaId == EventSchemaConstants.DUHE_SCHEMA_ID) {
      return EventSchemaConstants.DUHE_SCHEMA;
    } else {
      return null;
    }
  }

  @Override
  public Optional<Integer> getLatestSchemaVersionForTopic(String topicName) {
    Map<Integer, Schema> versionMap = _versionedSchemaMap.get(topicName);
    if (versionMap != null && !versionMap.isEmpty()) {
      return Optional.of(versionMap.keySet().stream().mapToInt(Integer::intValue).max().orElse(1));
    }
    return Optional.empty();
  }

  @Override
  public Optional<List<Integer>> getSupportedSchemaVersionsForTopic(String topicName) {
    Map<Integer, Schema> versionMap = _versionedSchemaMap.get(topicName);
    if (versionMap != null && !versionMap.isEmpty()) {
      return Optional.of(ImmutableList.copyOf(versionMap.keySet()));
    }
    return Optional.empty();
  }

  @Override
  public Optional<Integer> getSchemaIdForTopic(String topicName) {
    // Get the latest available schema ID for this topic
    Map<Integer, Integer> versionToIdMap = _versionToSchemaIdMap.get(topicName);
    if (versionToIdMap != null && !versionToIdMap.isEmpty()) {
      // Return the latest available schema ID (highest version number)
      int maxVersion = versionToIdMap.keySet().stream().mapToInt(Integer::intValue).max().orElse(1);
      return Optional.of(versionToIdMap.get(maxVersion));
    }
    return Optional.empty();
  }

  @Override
  public List<String> getAllTopics() {
    return ImmutableList.copyOf(_versionedSchemaMap.keySet());
  }

  @Override
  public String getSchemaCompatibility(String topicName) {
    return _schemaCompatibilityMap.getOrDefault(topicName, "NONE");
  }

  @Override
  public String getSchemaCompatibilityById(int schemaId) {
    final Optional<String> topicNameOpt = getTopicNameById(schemaId);
    if (!topicNameOpt.isPresent()) {
      return "NONE";
    }
    return getSchemaCompatibility(topicNameOpt.get());
  }

  @Override
  public Optional<String> getTopicNameById(final int schemaId) {
    return Optional.ofNullable(_schemaIdToTopicMap.get(schemaId));
  }

  /**
   * Get schema ID for a specific topic and version This is critical for proper schema resolution
   */
  public Optional<Integer> getSchemaIdForTopicAndVersion(String topicName, int version) {
    Map<Integer, Integer> versionToIdMap = _versionToSchemaIdMap.get(topicName);
    if (versionToIdMap != null) {
      return Optional.ofNullable(versionToIdMap.get(version));
    }
    return Optional.empty();
  }

  /**
   * Static utility method to calculate schema version from schema ID using centralized constants.
   * This can be used by other classes without needing an instance of SchemaRegistryServiceImpl.
   *
   * @param schemaName the schema name (e.g., "MetadataChangeLog")
   * @param schemaId the schema ID to find the version for
   * @return the version number (1-based), or 1 if not found
   */
  public static int getVersionForSchemaId(String schemaName, int schemaId) {
    Optional<List<Integer>> schemaIdsOpt =
        EventSchemaConstants.getSchemaIdsForSchemaName(schemaName);
    if (schemaIdsOpt.isPresent()) {
      List<Integer> schemaIds = schemaIdsOpt.get();
      for (int i = 0; i < schemaIds.size(); i++) {
        if (schemaIds.get(i) == schemaId) {
          return calculateVersionFromIndex(i);
        }
      }
    }
    return 1; // Default to version 1 if not found
  }

  @Override
  public Optional<Schema> getSchemaForTopic(String topicName) {
    // Return the latest version by default
    return getLatestSchemaVersionForTopic(topicName)
        .flatMap(version -> getSchemaForTopicAndVersion(topicName, version));
  }

  @Override
  public Optional<Schema> getSchemaForId(final int schemaId) {
    // Get the schema name for this ID
    Optional<String> schemaNameOpt = EventSchemaConstants.getSchemaNameForSchemaId(schemaId);
    if (schemaNameOpt.isEmpty()) {
      return Optional.empty();
    }

    String schemaName = schemaNameOpt.get();

    // Find which topic uses this schema and get the appropriate version
    for (Map.Entry<String, Map<Integer, Integer>> topicEntry : _versionToSchemaIdMap.entrySet()) {
      String topicName = topicEntry.getKey();
      Map<Integer, Integer> versionToIdMap = topicEntry.getValue();

      // Check if this topic has a schema ID that matches our target
      for (Map.Entry<Integer, Integer> versionEntry : versionToIdMap.entrySet()) {
        if (versionEntry.getValue().equals(schemaId)) {
          int version = versionEntry.getKey();
          Map<Integer, Schema> topicSchemas = _versionedSchemaMap.get(topicName);
          if (topicSchemas != null) {
            Schema schema = topicSchemas.get(version);
            if (schema != null) {
              return Optional.of(schema);
            }
          }
        }
      }
    }

    return Optional.empty();
  }

  private String getSchemaNameFromTopic(String topicName) {
    // Extract schema name from topic name (remove _v1 suffix)
    return topicName.replaceFirst("_v1$", "");
  }

  @Override
  public Optional<Schema> getSchemaForTopicAndVersion(String topicName, int version) {
    Map<Integer, Schema> versionMap = _versionedSchemaMap.get(topicName);
    if (versionMap != null) {
      return Optional.ofNullable(versionMap.get(version));
    }
    return Optional.empty();
  }

  @Override
  public Optional<Integer> getSchemaIdBySubjectAndVersion(String subject, int version) {
    // Convert subject (topic-value) to topic name
    String topicName = subject.replaceFirst("-value", "");

    // Get the schema ID for the topic and version
    Map<Integer, Integer> versionToIdMap = _versionToSchemaIdMap.get(topicName);
    if (versionToIdMap != null) {
      return Optional.ofNullable(versionToIdMap.get(version));
    }

    return Optional.empty();
  }

  @Override
  public Optional<Schema> getSchemaBySubjectAndVersion(String subject, int version) {
    // Convert subject (topic-value) to topic name
    String topicName = subject.replaceFirst("-value", "");

    // Get the schema for the topic and version
    return getSchemaForTopicAndVersion(topicName, version);
  }

  @Override
  public Optional<List<Integer>> getAllSchemaVersionsForSubject(String subject) {
    // Convert subject (topic-value) to topic name
    String topicName = subject.replaceFirst("-value", "");

    // Get all supported versions for the topic
    return getSupportedSchemaVersionsForTopic(topicName);
  }

  @Override
  public Optional<Integer> registerSchemaVersion(String topicName, Schema schema) {
    // This is a read-only schema registry, so we don't actually register new schemas
    // However, we can return the latest version if the schema matches an existing one
    Map<Integer, Schema> versionMap = _versionedSchemaMap.get(topicName);
    if (versionMap != null) {
      // Check if this schema already exists in any version
      for (Map.Entry<Integer, Schema> entry : versionMap.entrySet()) {
        if (entry.getValue().equals(schema)) {
          return Optional.of(entry.getKey());
        }
      }
    }

    // Schema not found, return empty (read-only mode)
    return Optional.empty();
  }

  @Override
  public boolean checkSchemaCompatibility(
      String topicName, Schema newSchema, Schema existingSchema) {
    // Get the compatibility level for this topic
    String compatibilityLevel = getSchemaCompatibility(topicName);

    switch (compatibilityLevel) {
      case EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE:
        // No compatibility guarantees - any change breaks compatibility
        return newSchema.equals(existingSchema);

      case EventSchemaConstants.SCHEMA_COMPATIBILITY_BACKWARD:
        // New schema must be backward compatible with existing data
        // This means new fields can be added, but existing fields cannot be removed or changed
        return checkBackwardCompatibility(newSchema, existingSchema);

      case EventSchemaConstants.SCHEMA_COMPATIBILITY_FORWARD:
        // New schema must be forward compatible with existing consumers
        // This means existing fields cannot be removed, but new fields can be added
        return checkForwardCompatibility(newSchema, existingSchema);

      case EventSchemaConstants.SCHEMA_COMPATIBILITY_FULL:
        // Must be both backward and forward compatible
        return checkBackwardCompatibility(newSchema, existingSchema)
            && checkForwardCompatibility(newSchema, existingSchema);

      default:
        // Unknown compatibility level, default to no compatibility
        return newSchema.equals(existingSchema);
    }
  }

  /**
   * Check if new schema is backward compatible with existing schema Backward compatibility: new
   * schema can read data written with old schema
   */
  private boolean checkBackwardCompatibility(Schema newSchema, Schema existingSchema) {
    // For now, implement a simple check - in a real implementation, you'd use Avro's compatibility
    // checker
    // This is a simplified version that checks if all existing fields are still present
    try {
      // Check if new schema can read data written with existing schema
      // This is a basic implementation - in production you'd want more sophisticated checking
      return newSchema.getFields().containsAll(existingSchema.getFields());
    } catch (Exception e) {
      // If compatibility check fails, assume incompatible
      return false;
    }
  }

  /**
   * Check if new schema is forward compatible with existing schema Forward compatibility: existing
   * consumers can read data written with new schema
   */
  private boolean checkForwardCompatibility(Schema newSchema, Schema existingSchema) {
    // For now, implement a simple check - in a real implementation, you'd use Avro's compatibility
    // checker
    try {
      // Check if existing schema can read data written with new schema
      // This is a basic implementation - in production you'd want more sophisticated checking
      return existingSchema.getFields().containsAll(newSchema.getFields());
    } catch (Exception e) {
      // If compatibility check fails, assume incompatible
      return false;
    }
  }
}
