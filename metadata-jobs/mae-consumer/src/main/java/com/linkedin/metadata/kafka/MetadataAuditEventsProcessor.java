package com.linkedin.metadata.kafka;

import com.linkedin.common.urn.Urn;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.gms.factory.common.GraphServiceFactory;
import com.linkedin.gms.factory.search.SearchServiceFactory;
import com.linkedin.gms.factory.usage.UsageServiceFactory;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.extractor.FieldExtractor;
import com.linkedin.metadata.graph.Edge;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.kafka.config.MetadataAuditEventsProcessorCondition;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.usage.UsageService;
import com.linkedin.mxe.MetadataAuditEvent;
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
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static com.linkedin.metadata.dao.Neo4jUtil.createRelationshipFilter;


@Slf4j
@Component
@Conditional(MetadataAuditEventsProcessorCondition.class)
@Import({GraphServiceFactory.class, SearchServiceFactory.class, UsageServiceFactory.class})
@EnableKafka
public class MetadataAuditEventsProcessor {

  private final GraphService _graphService;
  private final SearchService _searchService;
  private final UsageService _usageService;

  @Autowired
  public MetadataAuditEventsProcessor(GraphService graphService, SearchService searchService, UsageService usageService) {
    _graphService = graphService;
    _searchService = searchService;
    _usageService = usageService;

    _graphService.configure();
    _searchService.configure();
    _usageService.configure();
  }

  @KafkaListener(id = "${KAFKA_CONSUMER_GROUP_ID:mae-consumer-job-client}", topics = "${KAFKA_TOPIC_NAME:"
      + Topics.METADATA_AUDIT_EVENT + "}", containerFactory = "avroSerializedKafkaListener")
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    final GenericRecord record = consumerRecord.value();
    log.debug("Got MAE");

    try {
      final MetadataAuditEvent event = EventUtils.avroToPegasusMAE(record);
      if (event.hasNewSnapshot()) {
        final RecordTemplate snapshot = RecordUtils.getSelectedRecordTemplateFromUnion(event.getNewSnapshot());

        log.info(snapshot.toString());

        final EntitySpec entitySpec =
            SnapshotEntityRegistry.getInstance().getEntitySpec(PegasusUtils.getEntityNameFromSchema(snapshot.schema()));
        updateSearchService(snapshot, entitySpec);
        updateGraphService(snapshot, entitySpec);
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

    Map<String, List<RelationshipFieldSpec>> relationshipFieldSpecsPerAspect = entitySpec.getAspectSpecMap()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getRelationshipFieldSpecs()));
    Map<RelationshipFieldSpec, List<Object>> extractedFields =
        FieldExtractor.extractFields(snapshot, relationshipFieldSpecsPerAspect);

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
            createRelationshipFilter(new Filter().setCriteria(new CriterionArray()), RelationshipDirection.OUTGOING));
        edgesToAdd.forEach(edge -> _graphService.addEdge(edge));
      }).start();
    }
  }

  /**
   * Process snapshot and update Elasticsearch
   *
   * @param snapshot Snapshot
   */
  private void updateSearchService(final RecordTemplate snapshot, final EntitySpec entitySpec) {
    String urn = snapshot.data().get("urn").toString();
    Optional<String> searchDocument;
    try {
      searchDocument = SearchDocumentTransformer.transform(snapshot, entitySpec);
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

    _searchService.upsertDocument(entitySpec.getName(), searchDocument.get(), docId);
  }
}
