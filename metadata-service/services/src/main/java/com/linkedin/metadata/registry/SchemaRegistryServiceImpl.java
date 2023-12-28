package com.linkedin.metadata.registry;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.linkedin.mxe.TopicConvention;
import com.linkedin.pegasus2avro.mxe.DataHubUpgradeHistoryEvent;
import com.linkedin.pegasus2avro.mxe.FailedMetadataChangeEvent;
import com.linkedin.pegasus2avro.mxe.FailedMetadataChangeProposal;
import com.linkedin.pegasus2avro.mxe.MetadataAuditEvent;
import com.linkedin.pegasus2avro.mxe.MetadataChangeEvent;
import com.linkedin.pegasus2avro.mxe.MetadataChangeLog;
import com.linkedin.pegasus2avro.mxe.MetadataChangeProposal;
import com.linkedin.pegasus2avro.mxe.PlatformEvent;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.avro.Schema;

public class SchemaRegistryServiceImpl implements SchemaRegistryService {

  @AllArgsConstructor
  private enum TopicOrdinal {
    MCP_TOPIC(MetadataChangeProposal.getClassSchema()),
    FMCP_TOPIC(FailedMetadataChangeProposal.getClassSchema()),
    MCL_TOPIC(MetadataChangeLog.getClassSchema()),
    MCL_TIMESERIES_TOPIC(MetadataChangeLog.getClassSchema()),
    PE_TOPIC(PlatformEvent.getClassSchema()),
    MCE_TOPIC(MetadataChangeEvent.getClassSchema()),
    FMCE_TOPIC(FailedMetadataChangeEvent.getClassSchema()),
    MAE_TOPIC(MetadataAuditEvent.getClassSchema()),
    DUHE_TOPIC(DataHubUpgradeHistoryEvent.getClassSchema());

    @Getter private final Schema schema;
  }

  private final Map<String, Schema> _schemaMap;

  private final BiMap<String, Integer> _subjectToIdMap;

  public SchemaRegistryServiceImpl(final TopicConvention convention) {
    this._schemaMap = new HashMap<>();
    this._subjectToIdMap = HashBiMap.create();
    this._schemaMap.put(
        convention.getMetadataChangeProposalTopicName(), TopicOrdinal.MCP_TOPIC.getSchema());
    this._subjectToIdMap.put(
        convention.getMetadataChangeProposalTopicName(), TopicOrdinal.MCP_TOPIC.ordinal());
    this._schemaMap.put(
        convention.getMetadataChangeLogVersionedTopicName(), TopicOrdinal.MCL_TOPIC.getSchema());
    this._subjectToIdMap.put(
        convention.getMetadataChangeLogVersionedTopicName(), TopicOrdinal.MCL_TOPIC.ordinal());
    this._schemaMap.put(
        convention.getMetadataChangeLogTimeseriesTopicName(),
        TopicOrdinal.MCL_TIMESERIES_TOPIC.getSchema());
    this._subjectToIdMap.put(
        convention.getMetadataChangeLogTimeseriesTopicName(),
        TopicOrdinal.MCL_TIMESERIES_TOPIC.ordinal());
    this._schemaMap.put(
        convention.getFailedMetadataChangeProposalTopicName(), TopicOrdinal.FMCP_TOPIC.getSchema());
    this._subjectToIdMap.put(
        convention.getFailedMetadataChangeProposalTopicName(), TopicOrdinal.FMCP_TOPIC.ordinal());
    this._schemaMap.put(convention.getPlatformEventTopicName(), TopicOrdinal.PE_TOPIC.getSchema());
    this._subjectToIdMap.put(
        convention.getPlatformEventTopicName(), TopicOrdinal.PE_TOPIC.ordinal());
    this._schemaMap.put(
        convention.getDataHubUpgradeHistoryTopicName(), TopicOrdinal.DUHE_TOPIC.getSchema());
    this._subjectToIdMap.put(
        convention.getDataHubUpgradeHistoryTopicName(), TopicOrdinal.DUHE_TOPIC.ordinal());

    // Adding legacy topics as they are still produced in the EntityService IngestAspect code path.
    this._schemaMap.put(
        convention.getMetadataChangeEventTopicName(), TopicOrdinal.MCE_TOPIC.getSchema());
    this._subjectToIdMap.put(
        convention.getMetadataChangeEventTopicName(), TopicOrdinal.MCE_TOPIC.ordinal());
    this._schemaMap.put(
        convention.getFailedMetadataChangeEventTopicName(), TopicOrdinal.FMCE_TOPIC.getSchema());
    this._subjectToIdMap.put(
        convention.getFailedMetadataChangeEventTopicName(), TopicOrdinal.FMCE_TOPIC.ordinal());
    this._schemaMap.put(
        convention.getMetadataAuditEventTopicName(), TopicOrdinal.MAE_TOPIC.getSchema());
    this._subjectToIdMap.put(
        convention.getMetadataAuditEventTopicName(), TopicOrdinal.MAE_TOPIC.ordinal());
  }

  @Override
  public Optional<Integer> getSchemaIdForTopic(String topicName) {
    return Optional.ofNullable(_subjectToIdMap.get(topicName));
  }

  @Override
  public Optional<Schema> getSchemaForTopic(String topicName) {
    return Optional.ofNullable(_schemaMap.get(topicName));
  }

  @Override
  public Optional<Schema> getSchemaForId(int id) {
    final String topicName = _subjectToIdMap.inverse().get(id);
    return getSchemaForTopic(topicName);
  }
}
