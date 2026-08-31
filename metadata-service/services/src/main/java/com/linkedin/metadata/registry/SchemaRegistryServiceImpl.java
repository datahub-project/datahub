package com.linkedin.metadata.registry;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.EventSchemaConstants;
import com.linkedin.metadata.EventSchemaData;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.SchemaIdOrdinal;
import com.linkedin.mxe.TopicConvention;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.Schema;

/**
 * Schema Registry Service Implementation that maintains backward compatibility with existing schema
 * IDs.
 *
 * <p>Schema ID mapping is defined in {@link SchemaIdOrdinal} enum to maintain consistency and
 * prevent accidental changes to these critical values.
 */
public class SchemaRegistryServiceImpl implements SchemaRegistryService {

  private final EventSchemaData _eventSchemaData;
  private final TopicConvention _convention;
  private final Map<String, String> _topicToSchemaNameMap;

  public SchemaRegistryServiceImpl(
      final TopicConvention convention, final EventSchemaData eventSchemaData) {
    this._eventSchemaData = eventSchemaData;
    this._convention = convention;
    this._topicToSchemaNameMap = buildTopicToSchemaNameMap();
  }

  private String getSchemaNameFromTopic(String topicName) {
    return _topicToSchemaNameMap.get(topicName);
  }

  /**
   * Builds a map from topic names to schema names for efficient lookup. This replaces the long
   * if-else chain with a simple map lookup.
   */
  private Map<String, String> buildTopicToSchemaNameMap() {
    Map<String, String> map = new HashMap<>();
    map.put(
        _convention.getMetadataChangeProposalTopicName(),
        EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);
    map.put(
        _convention.getFailedMetadataChangeProposalTopicName(),
        EventUtils.FAILED_METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);
    map.put(
        _convention.getMetadataChangeLogVersionedTopicName(),
        EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME);
    map.put(
        _convention.getMetadataChangeLogTimeseriesTopicName(),
        EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME);
    map.put(_convention.getPlatformEventTopicName(), EventUtils.PLATFORM_EVENT_SCHEMA_NAME);
    map.put(
        _convention.getMetadataChangeEventTopicName(),
        EventUtils.METADATA_CHANGE_EVENT_SCHEMA_NAME);
    map.put(
        _convention.getFailedMetadataChangeEventTopicName(),
        EventUtils.FAILED_METADATA_CHANGE_EVENT_SCHEMA_NAME);
    map.put(
        _convention.getMetadataAuditEventTopicName(), EventUtils.METADATA_AUDIT_EVENT_SCHEMA_NAME);
    map.put(
        _convention.getDataHubUpgradeHistoryTopicName(),
        EventUtils.DATAHUB_UPGRADE_HISTORY_EVENT_SCHEMA_NAME);
    return map;
  }

  @Override
  public Optional<Integer> getLatestSchemaVersionForTopic(String topicName) {
    String schemaName = getSchemaNameFromTopic(topicName);
    if (schemaName == null) {
      return Optional.empty();
    }

    List<Integer> schemaIds = _eventSchemaData.getSchemaIdsForSchemaName(schemaName);
    if (schemaIds.isEmpty()) {
      return Optional.empty();
    }

    // Find the maximum version across all schema IDs
    int maxVersion = 0;
    for (int schemaId : schemaIds) {
      List<Integer> versions = _eventSchemaData.getVersionsForSchemaId(schemaId);
      if (!versions.isEmpty()) {
        int maxVersionForSchema = versions.stream().mapToInt(Integer::intValue).max().orElse(0);
        maxVersion = Math.max(maxVersion, maxVersionForSchema);
      }
    }

    return maxVersion > 0 ? Optional.of(maxVersion) : Optional.empty();
  }

  @Override
  public Optional<List<Integer>> getSupportedSchemaVersionsForTopic(String topicName) {
    String schemaName = getSchemaNameFromTopic(topicName);
    if (schemaName == null) {
      return Optional.empty();
    }

    List<Integer> schemaIds = _eventSchemaData.getSchemaIdsForSchemaName(schemaName);
    if (schemaIds.isEmpty()) {
      return Optional.empty();
    }

    // Collect all versions from all schema IDs
    List<Integer> allVersions = new ArrayList<>();
    for (int schemaId : schemaIds) {
      List<Integer> versions = _eventSchemaData.getVersionsForSchemaId(schemaId);
      allVersions.addAll(versions);
    }

    // Remove duplicates and sort
    List<Integer> uniqueVersions =
        allVersions.stream().distinct().sorted().collect(Collectors.toList());

    return uniqueVersions.isEmpty()
        ? Optional.empty()
        : Optional.of(ImmutableList.copyOf(uniqueVersions));
  }

  @Override
  public Optional<Integer> getSchemaIdForTopic(String topicName) {
    String schemaName = getSchemaNameFromTopic(topicName);
    if (schemaName == null) {
      return Optional.empty();
    }

    // Get all versions for this schema and return the highest version's schema registry ID
    List<Integer> schemaIds = _eventSchemaData.getSchemaIdsForSchemaName(schemaName);
    if (schemaIds.isEmpty()) {
      return Optional.empty();
    }

    // Find the highest version number for this schema
    int maxVersion = 0;
    for (int schemaId : schemaIds) {
      List<Integer> versions = _eventSchemaData.getVersionsForSchemaId(schemaId);
      if (!versions.isEmpty()) {
        maxVersion =
            Math.max(maxVersion, versions.stream().mapToInt(Integer::intValue).max().orElse(0));
      }
    }

    // Return the schema registry ID for the highest version
    return Optional.ofNullable(_eventSchemaData.getSchemaRegistryId(schemaName, maxVersion));
  }

  @Override
  public List<String> getAllTopics() {
    // Return all topic names from TopicConvention
    List<String> topics = new ArrayList<>();
    topics.add(_convention.getMetadataChangeProposalTopicName());
    topics.add(_convention.getFailedMetadataChangeProposalTopicName());
    topics.add(_convention.getMetadataChangeLogVersionedTopicName());
    // Note: MetadataChangeLogTimeseriesTopicName maps to the same schema as
    // MetadataChangeLogVersionedTopicName
    topics.add(_convention.getPlatformEventTopicName());
    topics.add(_convention.getMetadataChangeEventTopicName());
    topics.add(_convention.getFailedMetadataChangeEventTopicName());
    topics.add(_convention.getMetadataAuditEventTopicName());
    topics.add(_convention.getDataHubUpgradeHistoryTopicName());
    return ImmutableList.copyOf(topics);
  }

  @Override
  public String getSchemaCompatibility(String topicName) {
    String schemaName = getSchemaNameFromTopic(topicName);
    if (schemaName == null) {
      return "NONE";
    }
    return _eventSchemaData.getCompatibilityForSchemaName(schemaName);
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
    String schemaName = _eventSchemaData.getSchemaNameForSchemaId(schemaId);
    if (schemaName == null) {
      return Optional.empty();
    }

    // Map schema name back to topic name
    if (schemaName.equals(EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME)) {
      return Optional.of(_convention.getMetadataChangeProposalTopicName());
    } else if (schemaName.equals(EventUtils.FAILED_METADATA_CHANGE_PROPOSAL_SCHEMA_NAME)) {
      return Optional.of(_convention.getFailedMetadataChangeProposalTopicName());
    } else if (schemaName.equals(EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME)) {
      return Optional.of(_convention.getMetadataChangeLogVersionedTopicName());
    } else if (schemaName.equals(EventUtils.PLATFORM_EVENT_SCHEMA_NAME)) {
      return Optional.of(_convention.getPlatformEventTopicName());
    } else if (schemaName.equals(EventUtils.METADATA_CHANGE_EVENT_SCHEMA_NAME)) {
      return Optional.of(_convention.getMetadataChangeEventTopicName());
    } else if (schemaName.equals(EventUtils.FAILED_METADATA_CHANGE_EVENT_SCHEMA_NAME)) {
      return Optional.of(_convention.getFailedMetadataChangeEventTopicName());
    } else if (schemaName.equals(EventUtils.METADATA_AUDIT_EVENT_SCHEMA_NAME)) {
      return Optional.of(_convention.getMetadataAuditEventTopicName());
    } else if (schemaName.equals(EventUtils.DATAHUB_UPGRADE_HISTORY_EVENT_SCHEMA_NAME)) {
      return Optional.of(_convention.getDataHubUpgradeHistoryTopicName());
    }

    return Optional.empty();
  }

  /**
   * Get schema ID for a specific topic and version This is critical for proper schema resolution
   */
  public Optional<Integer> getSchemaIdForTopicAndVersion(String topicName, int version) {
    String schemaName = getSchemaNameFromTopic(topicName);
    if (schemaName == null) {
      return Optional.empty();
    }

    // Return the unique schema registry ID for this specific version
    return Optional.ofNullable(_eventSchemaData.getSchemaRegistryId(schemaName, version));
  }

  /**
   * Static utility method to calculate schema version from schema ID using centralized constants.
   * This can be used by other classes without needing an instance of SchemaRegistryServiceImpl.
   *
   * @param schemaName the schema name (e.g., "MetadataChangeLog")
   * @param schemaId the schema ID to find the version for
   * @return the version number (1-based), or 1 if not found
   */
  public int getVersionForSchemaId(String schemaName, int schemaId) {
    // Use EventSchemaData for O(1) lookup instead of array looping
    return _eventSchemaData.getVersionsForSchemaId(schemaId).stream()
        .mapToInt(Integer::intValue)
        .max()
        .orElse(1);
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
    String schemaName = _eventSchemaData.getSchemaNameForSchemaId(schemaId);
    if (schemaName == null) {
      return Optional.empty();
    }

    // Get all versions for this schema ID
    List<Integer> versions = _eventSchemaData.getVersionsForSchemaId(schemaId);
    if (versions.isEmpty()) {
      return Optional.empty();
    }

    // Get the versioned schemas for this schema ID
    Map<Integer, Schema> versionedSchemas =
        _eventSchemaData.getVersionedSchemasForSchemaId(schemaId);
    if (versionedSchemas == null || versionedSchemas.isEmpty()) {
      return Optional.empty();
    }

    // Return the first available schema (typically version 1)
    for (int version : versions) {
      Schema schema = versionedSchemas.get(version);
      if (schema != null) {
        return Optional.of(schema);
      }
    }

    return Optional.empty();
  }

  @Override
  public Optional<Schema> getSchemaForTopicAndVersion(String topicName, int version) {
    // Get schema name from topic name using TopicConvention
    String schemaName = getSchemaNameFromTopic(topicName);
    if (schemaName == null) {
      return Optional.empty();
    }

    // Get all schema IDs for this schema name
    List<Integer> schemaIds = _eventSchemaData.getSchemaIdsForSchemaName(schemaName);
    if (schemaIds.isEmpty()) {
      return Optional.empty();
    }

    // Find the schema ID that supports this version
    for (int schemaId : schemaIds) {
      List<Integer> versions = _eventSchemaData.getVersionsForSchemaId(schemaId);
      if (versions.contains(version)) {
        Map<Integer, Schema> versionedSchemas =
            _eventSchemaData.getVersionedSchemasForSchemaId(schemaId);
        if (versionedSchemas != null) {
          return Optional.ofNullable(versionedSchemas.get(version));
        }
      }
    }

    return Optional.empty();
  }

  @Override
  public Optional<Integer> getSchemaIdBySubjectAndVersion(String subject, int version) {
    // Convert subject (topic-value) to topic name
    String topicName = subject.replaceFirst("-value", "");

    // Use the existing method to get schema ID for topic and version
    return getSchemaIdForTopicAndVersion(topicName, version);
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
    String schemaName = getSchemaNameFromTopic(topicName);
    if (schemaName == null) {
      return Optional.empty();
    }

    List<Integer> schemaIds = _eventSchemaData.getSchemaIdsForSchemaName(schemaName);
    if (schemaIds.isEmpty()) {
      return Optional.empty();
    }

    // Check if this schema already exists in any version
    for (int schemaId : schemaIds) {
      Map<Integer, Schema> versionedSchemas =
          _eventSchemaData.getVersionedSchemasForSchemaId(schemaId);
      if (versionedSchemas != null) {
        for (Map.Entry<Integer, Schema> entry : versionedSchemas.entrySet()) {
          if (entry.getValue().equals(schema)) {
            return Optional.of(entry.getKey());
          }
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
