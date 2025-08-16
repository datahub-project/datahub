package com.linkedin.metadata.registry;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.EventSchemaConstants;
import com.linkedin.metadata.EventUtils;
import com.linkedin.mxe.TopicConvention;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;

public class SchemaRegistryServiceImpl implements SchemaRegistryService {

  private final Map<String, Map<Integer, Schema>> _versionedSchemaMap;
  private final BiMap<String, Integer> _subjectToIdMap;

  public SchemaRegistryServiceImpl(final TopicConvention convention) {
    this._versionedSchemaMap = new HashMap<>();
    this._subjectToIdMap = HashBiMap.create();

    // Initialize versioned schemas for MCP topics (each uses its own schema)
    initializeVersionedSchemas(convention);

    // Initialize single version schemas for other topics
    initializeSingleVersionSchemas(convention);
  }

  private void initializeVersionedSchemas(final TopicConvention convention) {
    // MCP topic uses MetadataChangeProposal schema
    Map<Integer, Schema> mcpSchemas = new HashMap<>();
    int latestMCPVersion =
        EventSchemaConstants.getLatestSchemaVersion(
            EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);
    mcpSchemas.put(EventSchemaConstants.SCHEMA_VERSION_1, EventSchemaConstants.MCP_V1_SCHEMA);
    mcpSchemas.put(latestMCPVersion, EventSchemaConstants.MCP_SCHEMA);

    // MCP topic
    _versionedSchemaMap.put(convention.getMetadataChangeProposalTopicName(), mcpSchemas);
    _subjectToIdMap.put(convention.getMetadataChangeProposalTopicName(), 0);

    // Failed MCP topic uses FailedMetadataChangeProposal schema
    Map<Integer, Schema> failedMcpSchemas = new HashMap<>();
    failedMcpSchemas.put(
        EventSchemaConstants.SCHEMA_VERSION_1, EventSchemaConstants.FMCP_V1_SCHEMA);
    failedMcpSchemas.put(
        EventSchemaConstants.getLatestSchemaVersion(
            EventUtils.FAILED_METADATA_CHANGE_PROPOSAL_SCHEMA_NAME),
        EventSchemaConstants.FMCP_SCHEMA);

    _versionedSchemaMap.put(
        convention.getFailedMetadataChangeProposalTopicName(), failedMcpSchemas);
    _subjectToIdMap.put(convention.getFailedMetadataChangeProposalTopicName(), 1);
  }

  private void initializeSingleVersionSchemas(final TopicConvention convention) {
    // Single version schemas for other topics - use latest versions dynamically
    _versionedSchemaMap.put(
        convention.getMetadataChangeLogVersionedTopicName(),
        Map.of(
            EventSchemaConstants.getLatestSchemaVersion(EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME),
            EventSchemaConstants.MCL_SCHEMA));
    _subjectToIdMap.put(convention.getMetadataChangeLogVersionedTopicName(), 2);

    _versionedSchemaMap.put(
        convention.getMetadataChangeLogTimeseriesTopicName(),
        Map.of(
            EventSchemaConstants.getLatestSchemaVersion(EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME),
            EventSchemaConstants.MCL_TIMESERIES_SCHEMA));
    _subjectToIdMap.put(convention.getMetadataChangeLogTimeseriesTopicName(), 3);

    _versionedSchemaMap.put(
        convention.getPlatformEventTopicName(),
        Map.of(
            EventSchemaConstants.getLatestSchemaVersion(EventUtils.PLATFORM_EVENT_SCHEMA_NAME),
            EventSchemaConstants.PE_SCHEMA));
    _subjectToIdMap.put(convention.getPlatformEventTopicName(), 4);

    _versionedSchemaMap.put(
        convention.getDataHubUpgradeHistoryTopicName(),
        Map.of(
            EventSchemaConstants.getLatestSchemaVersion(
                EventUtils.DATAHUB_UPGRADE_HISTORY_EVENT_SCHEMA_NAME),
            EventSchemaConstants.DUHE_SCHEMA));
    _subjectToIdMap.put(convention.getDataHubUpgradeHistoryTopicName(), 5);

    // Legacy topics
    _versionedSchemaMap.put(
        convention.getMetadataChangeEventTopicName(),
        Map.of(
            EventSchemaConstants.getLatestSchemaVersion(
                EventUtils.METADATA_CHANGE_EVENT_SCHEMA_NAME),
            EventSchemaConstants.MCE_SCHEMA));
    _subjectToIdMap.put(convention.getMetadataChangeEventTopicName(), 6);

    _versionedSchemaMap.put(
        convention.getFailedMetadataChangeEventTopicName(),
        Map.of(
            EventSchemaConstants.getLatestSchemaVersion(
                EventUtils.FAILED_METADATA_CHANGE_EVENT_SCHEMA_NAME),
            EventSchemaConstants.FMCE_SCHEMA));
    _subjectToIdMap.put(convention.getFailedMetadataChangeEventTopicName(), 7);

    _versionedSchemaMap.put(
        convention.getMetadataAuditEventTopicName(),
        Map.of(
            EventSchemaConstants.getLatestSchemaVersion(
                EventUtils.METADATA_AUDIT_EVENT_SCHEMA_NAME),
            EventSchemaConstants.MAE_SCHEMA));
    _subjectToIdMap.put(convention.getMetadataAuditEventTopicName(), 8);
  }

  @Override
  public Optional<Integer> getSchemaIdForTopic(String topicName) {
    return Optional.ofNullable(_subjectToIdMap.get(topicName));
  }

  @Override
  public Optional<Schema> getSchemaForTopic(String topicName) {
    // Return the latest version by default
    return getLatestSchemaVersionForTopic(topicName)
        .flatMap(version -> getSchemaForTopicAndVersion(topicName, version));
  }

  @Override
  public Optional<Schema> getSchemaForId(int id) {
    final String topicName = _subjectToIdMap.inverse().get(id);
    return getSchemaForTopic(topicName);
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
  public Optional<Integer> getLatestSchemaVersionForTopic(String topicName) {
    Map<Integer, Schema> versionMap = _versionedSchemaMap.get(topicName);
    if (versionMap != null && !versionMap.isEmpty()) {
      return Optional.of(
          versionMap.keySet().stream()
              .mapToInt(Integer::intValue)
              .max()
              .orElse(EventSchemaConstants.SCHEMA_VERSION_1));
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
}
