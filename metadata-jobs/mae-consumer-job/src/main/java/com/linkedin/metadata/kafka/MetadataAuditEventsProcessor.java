package com.linkedin.metadata.kafka;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.builders.search.BaseIndexBuilder;
import com.linkedin.metadata.builders.search.SnapshotProcessor;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.extractor.FieldExtractor;
import com.linkedin.metadata.graph.Neo4jGraphDAO;
import com.linkedin.metadata.kafka.elasticsearch.ElasticsearchConnector;
import com.linkedin.metadata.kafka.elasticsearch.MCEElasticEvent;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.search.indexbuilder.IndexBuilder;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.mxe.MetadataAuditEvent;
import com.linkedin.mxe.Topics;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Slf4j
@Component
@EnableKafka
public class MetadataAuditEventsProcessor {

  private RestHighLevelClient elasticSearchClient;
  private ElasticsearchConnector elasticSearchConnector;
  private SnapshotProcessor snapshotProcessor;

  private Neo4jGraphDAO _graphQueryDao;

  private Set<BaseIndexBuilder<? extends RecordTemplate>> indexBuilders;
  private IndexConvention indexConvention;

  public MetadataAuditEventsProcessor(RestHighLevelClient elasticSearchClient,
      ElasticsearchConnector elasticSearchConnector, SnapshotProcessor snapshotProcessor, Neo4jGraphDAO graphWriterDAO,
      Set<BaseIndexBuilder<? extends RecordTemplate>> indexBuilders, IndexConvention indexConvention) {
    this.elasticSearchClient = elasticSearchClient;
    this.elasticSearchConnector = elasticSearchConnector;
    this.snapshotProcessor = snapshotProcessor;
    this._graphQueryDao = graphWriterDAO;
    this.indexBuilders = indexBuilders;
    this.indexConvention = indexConvention;
    log.info("registered index builders {}", indexBuilders);
    try {
      new IndexBuilder(elasticSearchClient, new SnapshotEntityRegistry().getEntitySpec("testEntity"),
          "testentity").buildIndex();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @KafkaListener(id = "${KAFKA_CONSUMER_GROUP_ID:mae-consumer-job-client}", topics = "${KAFKA_TOPIC_NAME:"
      + Topics.METADATA_AUDIT_EVENT + "}", containerFactory = "avroSerializedKafkaListener")
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    final GenericRecord record = consumerRecord.value();
    log.debug("Got MAE");

    try {
      final MetadataAuditEvent event = EventUtils.avroToPegasusMAE(record);
      if (event.hasNewSnapshot()) {
        final Snapshot snapshot = event.getNewSnapshot();

        log.info(snapshot.toString());

        updateElasticsearch(snapshot);
        updateNeo4j(RecordUtils.getSelectedRecordTemplateFromUnion(snapshot));
      }
    } catch (Exception e) {
      log.error("Error deserializing message: {}", e.toString());
      log.error("Message: {}", record.toString());
    }
  }

  /**
   * Process snapshot and update Neo4j
   *
   * @param snapshot Snapshot
   */
  private void updateNeo4j(final RecordTemplate snapshot) {
    // TODO(Gabe): memoize this
    final EntitySpec entitySpec = EntitySpecBuilder.buildEntitySpec(snapshot.schema());
    Map<String, List<RelationshipFieldSpec>> relationshipFieldSpecsPerAspect = entitySpec.getAspectSpecMap()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getRelationshipFieldSpecs()));
    Map<RelationshipFieldSpec, Object> extractedFields =
        FieldExtractor.extractFields(snapshot, relationshipFieldSpecsPerAspect);

    for (Map.Entry<RelationshipFieldSpec, Object> entry : extractedFields.entrySet()) {
      try {
        _graphQueryDao.addAbstractEdge(Urn.createFromString(snapshot.data().get("urn").toString()),
            Urn.createFromString(entry.getValue().toString()), entry.getKey().getRelationshipName());
      } catch (URISyntaxException e) {
        log.info("Invalid urn: {}", e.getLocalizedMessage());
      }
    }
  }

  /**
   * Process snapshot and update Elasticsearch
   *
   * @param snapshot Snapshot
   */
  private void updateElasticsearch(final Snapshot snapshot) {
    List<RecordTemplate> docs = new ArrayList<>();
    try {
      docs = snapshotProcessor.getDocumentsToUpdate(snapshot);
    } catch (Exception e) {
      log.error("Error in getting documents from snapshot: {}", e.toString());
    }

    for (RecordTemplate doc : docs) {
      MCEElasticEvent elasticEvent = new MCEElasticEvent(doc);
      BaseIndexBuilder indexBuilderForDoc = null;
      for (BaseIndexBuilder indexBuilder : indexBuilders) {
        Class docType = indexBuilder.getDocumentType();
        if (docType.isInstance(doc)) {
          indexBuilderForDoc = indexBuilder;
          break;
        }
      }
      if (indexBuilderForDoc == null) {
        continue;
      }
      elasticEvent.setIndex(indexConvention.getIndexName(indexBuilderForDoc.getDocumentType()));
      try {
        String urn = indexBuilderForDoc.getDocumentType().getMethod("getUrn").invoke(doc).toString();
        elasticEvent.setId(URLEncoder.encode(urn.toLowerCase(), "UTF-8"));
      } catch (UnsupportedEncodingException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
        log.error("Failed to encode the urn with error: {}", e.toString());
        continue;
      }
      elasticEvent.setActionType(ChangeType.UPDATE);
      elasticSearchConnector.feedElasticEvent(elasticEvent);
    }
  }
}
