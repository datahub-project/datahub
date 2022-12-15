package com.linkedin.metadata.schema.registry;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.linkedin.mxe.TopicConvention;
import com.linkedin.pegasus2avro.mxe.FailedMetadataChangeProposal;
import com.linkedin.pegasus2avro.mxe.MetadataChangeLog;
import com.linkedin.pegasus2avro.mxe.MetadataChangeProposal;
import com.linkedin.pegasus2avro.mxe.PlatformEvent;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;


public class SchemaRegistryServiceImpl implements SchemaRegistryService {

  private final Map<String, Schema> _schemaMap;

  private final BiMap<String, Integer> _subjectToIdMap;

  public SchemaRegistryServiceImpl(final TopicConvention convention) {
    this._schemaMap = new HashMap<>();
    this._subjectToIdMap = HashBiMap.create();
    this._schemaMap.put(convention.getMetadataChangeProposalTopicName(), MetadataChangeProposal.getClassSchema());
    this._subjectToIdMap.put(convention.getMetadataChangeProposalTopicName(), 1);
    this._schemaMap.put(convention.getMetadataChangeLogVersionedTopicName(), MetadataChangeLog.getClassSchema());
    this._subjectToIdMap.put(convention.getMetadataChangeLogVersionedTopicName(), 2);
    this._schemaMap.put(convention.getMetadataChangeLogTimeseriesTopicName(), MetadataChangeLog.getClassSchema());
    this._subjectToIdMap.put(convention.getMetadataChangeLogTimeseriesTopicName(), 3);
    this._schemaMap.put(convention.getFailedMetadataChangeProposalTopicName(),
        FailedMetadataChangeProposal.getClassSchema());
    this._subjectToIdMap.put(convention.getFailedMetadataChangeProposalTopicName(), 4);
    this._schemaMap.put(convention.getPlatformEventTopicName(), PlatformEvent.getClassSchema());
    this._subjectToIdMap.put(convention.getPlatformEventTopicName(), 5);
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
