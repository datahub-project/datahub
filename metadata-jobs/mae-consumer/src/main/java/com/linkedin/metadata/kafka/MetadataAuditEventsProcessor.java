package com.linkedin.metadata.kafka;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.element.DataElement;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.gms.factory.common.GraphServiceFactory;
import com.linkedin.gms.factory.common.SystemMetadataServiceFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.extractor.AspectExtractor;
import com.linkedin.metadata.extractor.FieldExtractor;
import com.linkedin.metadata.graph.Edge;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.kafka.config.MetadataChangeLogProcessorCondition;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataAuditEvent;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.mxe.Topics;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static com.linkedin.metadata.search.utils.QueryUtils.createRelationshipFilter;


@Slf4j
@Component
@Conditional(MetadataChangeLogProcessorCondition.class)
@Import({GraphServiceFactory.class, EntitySearchServiceFactory.class, SystemMetadataServiceFactory.class})
@EnableKafka
public class MetadataAuditEventsProcessor {

  private final GraphService _graphService;
  private final EntitySearchService _entitySearchService;
  private final SystemMetadataService _systemMetadataService;

  private final Histogram kafkaLagStats = MetricUtils.get().histogram(MetricRegistry.name(this.getClass(), "kafkaLag"));

  @Autowired
  public MetadataAuditEventsProcessor(GraphService graphService, EntitySearchService entitySearchService,
      SystemMetadataService systemMetadataService) {
    _graphService = graphService;
    _entitySearchService = entitySearchService;
    _systemMetadataService = systemMetadataService;

    _graphService.configure();
    _entitySearchService.configure();
    _systemMetadataService.configure();
  }

  @KafkaListener(id = "${METADATA_AUDIT_EVENT_KAFKA_CONSUMER_GROUP_ID:mae-consumer-job-client}", topics =
      "${KAFKA_TOPIC_NAME:" + Topics.METADATA_AUDIT_EVENT + "}", containerFactory = "avroSerializedKafkaListener")
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    kafkaLagStats.update(System.currentTimeMillis() - consumerRecord.timestamp());

    final GenericRecord record = consumerRecord.value();
    log.debug("Got MAE");

    try {
      final MetadataAuditEvent event = EventUtils.avroToPegasusMAE(record);

      final RecordTemplate snapshot = RecordUtils.getSelectedRecordTemplateFromUnion(event.getNewSnapshot());

      log.info(snapshot.toString());

      final EntitySpec entitySpec =
          SnapshotEntityRegistry.getInstance().getEntitySpec(PegasusUtils.getEntityNameFromSchema(snapshot.schema()));
      updateSearchService(snapshot, entitySpec);
      updateGraphService(snapshot, entitySpec);
      updateSystemMetadata(snapshot, event.getNewSystemMetadata(), entitySpec);
    } catch (Exception e) {
      log.error("Error deserializing message: {}", e.toString());
      log.error("Message: {}", record.toString());
    }
  }

  private void updateSystemMetadata(
      @Nullable final RecordTemplate newSnapshot, @Nullable final SystemMetadata newSystemMetadata,
      @Nonnull final EntitySpec entitySpec) {

    // otherwise, we want to update the index with a new run id
    if (newSnapshot != null) {
      Map<String, DataElement> newAspects = AspectExtractor.extractAspects(newSnapshot);

      final String newUrn = newSnapshot.data().get("urn").toString();

      newAspects.keySet().forEach(aspect -> {
        // an MAE containing just a key signifies that the entity is being created- only then should we persist the key
        // run id pair
        if (!aspect.equals(entitySpec.getKeyAspectName()) || newAspects.keySet().size() == 1) {
          _systemMetadataService.insert(newSystemMetadata, newUrn, aspect);
        }
      });
    }
  }

  /**
   * Process snapshot and update graph index
   *
   * @param snapshot Snapshot
   */
  private void updateGraphService(final RecordTemplate snapshot, final EntitySpec entitySpec) {
    final Set<String> relationshipTypesBeingAdded = new HashSet<>();
    final List<Edge> edgesToAdd = new ArrayList<>();
    final String sourceUrnStr = snapshot.data().get("urn").toString();
    Urn sourceUrn;
    try {
      sourceUrn = Urn.createFromString(sourceUrnStr);
    } catch (URISyntaxException e) {
      log.info("Invalid source urn: {}", e.getLocalizedMessage());
      return;
    }

    Map<RelationshipFieldSpec, List<Object>> extractedFields =
        FieldExtractor.extractFieldsFromSnapshot(snapshot, entitySpec, AspectSpec::getRelationshipFieldSpecs);

    for (Map.Entry<RelationshipFieldSpec, List<Object>> entry : extractedFields.entrySet()) {
      relationshipTypesBeingAdded.add(entry.getKey().getRelationshipName());
      for (Object fieldValue : entry.getValue()) {
        try {
          edgesToAdd.add(
              new Edge(sourceUrn, Urn.createFromString(fieldValue.toString()), entry.getKey().getRelationshipName()));
        } catch (URISyntaxException e) {
          log.info("Invalid destination urn: {}", e.getLocalizedMessage());
        }
      }
    }

    if (edgesToAdd.size() > 0) {
      new Thread(() -> {
        _graphService.removeEdgesFromNode(sourceUrn, new ArrayList<>(relationshipTypesBeingAdded),
            createRelationshipFilter(new Filter().setOr(new ConjunctiveCriterionArray()), RelationshipDirection.OUTGOING));
        edgesToAdd.forEach(edge -> _graphService.addEdge(edge));
      }).start();
    }
  }

  /**
   * Process snapshot and update search index
   *
   * @param snapshot Snapshot
   */
  private void updateSearchService(final RecordTemplate snapshot, final EntitySpec entitySpec) {
    String urn = snapshot.data().get("urn").toString();
    Optional<String> searchDocument;

    try {
      searchDocument = SearchDocumentTransformer.transformSnapshot(snapshot, entitySpec, false);
    } catch (Exception e) {
      log.error("Error in getting documents from snapshot: {} for snapshot {}", e, snapshot);
      return;
    }

    if (!searchDocument.isPresent()) {
      return;
    }

    String docId;
    try {
      docId = URLEncoder.encode(urn, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      log.error("Failed to encode the urn with error: {}", e.toString());
      return;
    }

    _entitySearchService.upsertDocument(entitySpec.getName(), searchDocument.get(), docId);
  }
}
