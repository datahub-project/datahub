package com.linkedin.metadata.kafka;

import com.linkedin.data.DataMap;
import com.linkedin.data.element.DataElement;
import com.linkedin.data.it.IterationOrder;
import com.linkedin.data.it.ObjectIterator;
import com.linkedin.data.schema.PathSpec;
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
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.search.index_builder.IndexBuilder;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.kafka.elasticsearch.ElasticsearchConnector;
import com.linkedin.metadata.kafka.elasticsearch.MCEElasticEvent;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.mxe.MetadataAuditEvent;
import com.linkedin.mxe.Topics;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
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
  private BaseGraphWriterDAO graphWriterDAO;
  private Set<BaseIndexBuilder<? extends RecordTemplate>> indexBuilders;
  private IndexConvention indexConvention;

  public MetadataAuditEventsProcessor(RestHighLevelClient elasticSearchClient, ElasticsearchConnector elasticSearchConnector,
      SnapshotProcessor snapshotProcessor, BaseGraphWriterDAO graphWriterDAO,
      Set<BaseIndexBuilder<? extends RecordTemplate>> indexBuilders, IndexConvention indexConvention) {
    this.elasticSearchClient = elasticSearchClient;
    this.elasticSearchConnector = elasticSearchConnector;
    this.snapshotProcessor = snapshotProcessor;
    this.graphWriterDAO = graphWriterDAO;
    this.indexBuilders = indexBuilders;
    this.indexConvention = indexConvention;
    log.info("registered index builders {}", indexBuilders);
    try {
      new IndexBuilder(elasticSearchClient, new SnapshotEntityRegistry().getEntitySpec("testEntity"), "testentity").buildIndex();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @KafkaListener(id = "${KAFKA_CONSUMER_GROUP_ID:mae-consumer-job-client}", topics = "${KAFKA_TOPIC_NAME:"
      + Topics.METADATA_AUDIT_EVENT + "}")
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
    ObjectIterator dataElement = new ObjectIterator(snapshot.data(),
            snapshot.schema(),
            IterationOrder.PRE_ORDER);
    while (true) {
      final DataElement next = dataElement.next();
      final PathSpec pathSpec = next.getSchemaPathSpec();
      List<String> pathComponents = pathSpec.getPathComponents();
      if (pathComponents.size() < 4) {
        continue;
      }
      final String aspectName = pathComponents.get(2);
      final String suffix = "/" + StringUtils.join(pathComponents.subList(3, pathComponents.size()), "/");
      final Optional<RelationshipFieldSpec> matchingAnnotation = entitySpec
              .getAspectSpecMap()
              .get(aspectName)
              .getRelationshipFieldSpecs().stream().filter(fieldSpec -> fieldSpec.getPath().toString().equals(suffix)).findAny();
      if (matchingAnnotation.isPresent()) {
        try {
          graphWriterDAO.addAbstractEdge(
                  (String) snapshot.data().get("urn"),
                  (String) ((DataMap) next.getValue()).get("entity"),
                  matchingAnnotation.get().getRelationshipName(),
                  new HashMap<>()
          );
        } catch (URISyntaxException e) {
          e.printStackTrace();
        }
      }
      if (next == null) {
        break;
      }
    }

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
