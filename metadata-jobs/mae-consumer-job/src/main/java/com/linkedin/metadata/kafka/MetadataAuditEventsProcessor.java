package com.linkedin.metadata.kafka;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.builders.graph.BaseGraphBuilder;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.builders.graph.RegisteredGraphBuilders;
import com.linkedin.metadata.builders.search.BaseIndexBuilder;
import com.linkedin.metadata.builders.search.SnapshotProcessor;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.kafka.elasticsearch.ElasticsearchConnector;
import com.linkedin.metadata.kafka.elasticsearch.MCEElasticEvent;
import com.linkedin.mxe.MetadataAuditEvent;
import com.linkedin.mxe.Topics;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Slf4j
@Component
@EnableKafka
public class MetadataAuditEventsProcessor {

  private ElasticsearchConnector elasticSearchConnector;
  private SnapshotProcessor snapshotProcessor;
  private BaseGraphWriterDAO graphWriterDAO;
  private Set<BaseIndexBuilder<? extends RecordTemplate>> indexBuilders;

  public MetadataAuditEventsProcessor(ElasticsearchConnector elasticSearchConnector,
      SnapshotProcessor snapshotProcessor, BaseGraphWriterDAO graphWriterDAO,
      Set<BaseIndexBuilder<? extends RecordTemplate>> indexBuilders) {
    this.elasticSearchConnector = elasticSearchConnector;
    this.snapshotProcessor = snapshotProcessor;
    this.graphWriterDAO = graphWriterDAO;
    this.indexBuilders = indexBuilders;
    log.info("registered index builders {}", indexBuilders);
  }

  @KafkaListener(id = "mae-consumer-job-client", topics = "${KAFKA_TOPIC_NAME:" + Topics.METADATA_AUDIT_EVENT + "}")
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
    try {
      final BaseGraphBuilder graphBuilder = RegisteredGraphBuilders.getGraphBuilder(snapshot.getClass()).get();
      final GraphBuilder.GraphUpdates updates = graphBuilder.build(snapshot);

      if (!updates.getEntities().isEmpty()) {
        graphWriterDAO.addEntities(updates.getEntities());
      }

      for (GraphBuilder.RelationshipUpdates update : updates.getRelationshipUpdates()) {
        graphWriterDAO.addRelationships(update.getRelationships(), update.getPreUpdateOperation());
      }
    } catch (Exception ex) {
      log.error(ex.toString() + " " + Arrays.toString(ex.getStackTrace()));
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
      elasticEvent.setIndex(indexBuilderForDoc.getDocumentType().getSimpleName().toLowerCase());
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
