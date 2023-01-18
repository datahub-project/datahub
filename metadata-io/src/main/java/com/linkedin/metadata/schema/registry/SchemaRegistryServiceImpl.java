package com.linkedin.metadata.schema.registry;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.linkedin.mxe.TopicConvention;
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
  private enum TOPIC_ORDINAL {
    MCP_TOPIC(MetadataChangeProposal.getClassSchema()),
    FMCP_TOPIC(FailedMetadataChangeProposal.getClassSchema()),
    MCL_TOPIC(MetadataChangeLog.getClassSchema()),
    MCL_TIMESERIES_TOPIC(MetadataChangeLog.getClassSchema()),
    PE_TOPIC(PlatformEvent.getClassSchema()),
    MCE_TOPIC(MetadataChangeEvent.getClassSchema()),
    FMCE_TOPIC(FailedMetadataChangeEvent.getClassSchema()),
    MAE_TOPIC(MetadataAuditEvent.getClassSchema());

    @Getter
    private final Schema schema;
  }

  private final Map<String, Schema> _schemaMap;

  private final BiMap<String, Integer> _subjectToIdMap;

  public SchemaRegistryServiceImpl(final TopicConvention convention) {
    this._schemaMap = new HashMap<>();
    this._subjectToIdMap = HashBiMap.create();
    this._schemaMap.put(convention.getMetadataChangeProposalTopicName(), TOPIC_ORDINAL.MCP_TOPIC.getSchema());
    this._subjectToIdMap.put(convention.getMetadataChangeProposalTopicName(), TOPIC_ORDINAL.MCP_TOPIC.ordinal());
    this._schemaMap.put(convention.getMetadataChangeLogVersionedTopicName(), TOPIC_ORDINAL.MCL_TOPIC.getSchema());
    this._subjectToIdMap.put(convention.getMetadataChangeLogVersionedTopicName(), TOPIC_ORDINAL.MCL_TOPIC.ordinal());
    this._schemaMap.put(convention.getMetadataChangeLogTimeseriesTopicName(),
        TOPIC_ORDINAL.MCL_TIMESERIES_TOPIC.getSchema());
    this._subjectToIdMap.put(convention.getMetadataChangeLogTimeseriesTopicName(),
        TOPIC_ORDINAL.MCL_TIMESERIES_TOPIC.ordinal());
    this._schemaMap.put(convention.getFailedMetadataChangeProposalTopicName(), TOPIC_ORDINAL.FMCP_TOPIC.getSchema());
    this._subjectToIdMap.put(convention.getFailedMetadataChangeProposalTopicName(), TOPIC_ORDINAL.FMCP_TOPIC.ordinal());
    this._schemaMap.put(convention.getPlatformEventTopicName(), TOPIC_ORDINAL.PE_TOPIC.getSchema());
    this._subjectToIdMap.put(convention.getPlatformEventTopicName(), TOPIC_ORDINAL.PE_TOPIC.ordinal());

    // Adding legacy topics as they are still produced in the EntityService IngestAspect code path.
    this._schemaMap.put(convention.getMetadataChangeEventTopicName(), TOPIC_ORDINAL.MCE_TOPIC.getSchema());
    this._subjectToIdMap.put(convention.getMetadataChangeEventTopicName(), TOPIC_ORDINAL.MCE_TOPIC.ordinal());
    this._schemaMap.put(convention.getFailedMetadataChangeEventTopicName(), TOPIC_ORDINAL.FMCE_TOPIC.getSchema());
    this._subjectToIdMap.put(convention.getFailedMetadataChangeEventTopicName(), TOPIC_ORDINAL.FMCE_TOPIC.ordinal());
    this._schemaMap.put(convention.getMetadataAuditEventTopicName(), TOPIC_ORDINAL.MAE_TOPIC.getSchema());
    this._subjectToIdMap.put(convention.getMetadataAuditEventTopicName(), TOPIC_ORDINAL.MAE_TOPIC.ordinal());
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
