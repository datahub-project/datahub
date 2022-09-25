package com.linkedin.metadata.kafka.hook;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.common.GraphServiceFactory;
import com.linkedin.gms.factory.common.SystemMetadataServiceFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.gms.factory.search.SearchDocumentTransformerFactory;
import com.linkedin.gms.factory.timeseries.TimeseriesAspectServiceFactory;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.Edge;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.SearchUtils;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.transformer.TimeseriesAspectTransformer;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
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
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

import static com.linkedin.metadata.search.utils.QueryUtils.*;

@Slf4j
@Component
@Import({GraphServiceFactory.class, EntitySearchServiceFactory.class, TimeseriesAspectServiceFactory.class,
    EntityRegistryFactory.class, SystemMetadataServiceFactory.class, SearchDocumentTransformerFactory.class})
public class UpdateIndicesHook implements MetadataChangeLogHook {

  private final GraphService _graphService;
  private final EntitySearchService _entitySearchService;
  private final TimeseriesAspectService _timeseriesAspectService;
  private final SystemMetadataService _systemMetadataService;
  private final EntityRegistry _entityRegistry;
  private final SearchDocumentTransformer _searchDocumentTransformer;

  private static final Set<ChangeType> VALID_CHANGE_TYPES =
      Stream.of(
          ChangeType.UPSERT,
          ChangeType.RESTATE,
          ChangeType.PATCH).collect(Collectors.toSet());

  @Autowired
  public UpdateIndicesHook(
      GraphService graphService,
      EntitySearchService entitySearchService,
      TimeseriesAspectService timeseriesAspectService,
      SystemMetadataService systemMetadataService,
      EntityRegistry entityRegistry,
      SearchDocumentTransformer searchDocumentTransformer) {
    _graphService = graphService;
    _entitySearchService = entitySearchService;
    _timeseriesAspectService = timeseriesAspectService;
    _systemMetadataService = systemMetadataService;
    _entityRegistry = entityRegistry;
    _searchDocumentTransformer = searchDocumentTransformer;
    _graphService.configure();
    _entitySearchService.configure();
    _systemMetadataService.configure();
    _timeseriesAspectService.configure();
  }

  @Override
  public void invoke(@Nonnull MetadataChangeLog event) {
    EntitySpec entitySpec;
    try {
      entitySpec = _entityRegistry.getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      return;
    }
    Urn urn = EntityKeyUtils.getUrnFromLog(event, entitySpec.getKeyAspectSpec());

    if (VALID_CHANGE_TYPES.contains(event.getChangeType())) {

      if (!event.hasAspectName() || !event.hasAspect()) {
        log.error("Aspect or aspect name is missing");
        return;
      }

      AspectSpec aspectSpec = entitySpec.getAspectSpec(event.getAspectName());
      if (aspectSpec == null) {
        log.error("Unrecognized aspect name {} for entity {}", event.getAspectName(), event.getEntityType());
        return;
      }

      RecordTemplate aspect =
          GenericRecordUtils.deserializeAspect(event.getAspect().getValue(), event.getAspect().getContentType(),
              aspectSpec);
      if (aspectSpec.isTimeseries()) {
        updateTimeseriesFields(event.getEntityType(), event.getAspectName(), urn, aspect, aspectSpec,
            event.getSystemMetadata());
      } else {
        updateSearchService(entitySpec.getName(), urn, aspectSpec, aspect, event.hasSystemMetadata() ? event.getSystemMetadata().getRunId() : null);
        updateGraphService(urn, aspectSpec, aspect);
        updateSystemMetadata(event.getSystemMetadata(), urn, aspectSpec, aspect);
      }
    } else if (event.getChangeType() == ChangeType.DELETE) {
      if (!event.hasAspectName() || !event.hasPreviousAspectValue()) {
        log.error("Previous aspect or aspect name is missing");
        return;
      }

      AspectSpec aspectSpec = entitySpec.getAspectSpec(event.getAspectName());
      if (aspectSpec == null) {
        log.error("Unrecognized aspect name {} for entity {}", event.getAspectName(), event.getEntityType());
        return;
      }

      RecordTemplate aspect = GenericRecordUtils.deserializeAspect(event.getPreviousAspectValue().getValue(),
              event.getPreviousAspectValue().getContentType(), aspectSpec);
      Boolean isDeletingKey = event.getAspectName().equals(entitySpec.getKeyAspectName());

      if (!aspectSpec.isTimeseries()) {
        deleteSystemMetadata(urn, aspectSpec, isDeletingKey);
        deleteGraphData(urn, aspectSpec, aspect, isDeletingKey);
        deleteSearchData(urn, entitySpec.getName(), aspectSpec, aspect, isDeletingKey);
      }
    }
  }

  private Pair<List<Edge>, Set<String>> getEdgesAndRelationshipTypesFromAspect(Urn urn, AspectSpec aspectSpec, RecordTemplate aspect) {
    final Set<String> relationshipTypesBeingAdded = new HashSet<>();
    final List<Edge> edgesToAdd = new ArrayList<>();

    Map<RelationshipFieldSpec, List<Object>> extractedFields =
        FieldExtractor.extractFields(aspect, aspectSpec.getRelationshipFieldSpecs());

    for (Map.Entry<RelationshipFieldSpec, List<Object>> entry : extractedFields.entrySet()) {
      relationshipTypesBeingAdded.add(entry.getKey().getRelationshipName());
      for (Object fieldValue : entry.getValue()) {
        try {
          edgesToAdd.add(
              new Edge(urn, Urn.createFromString(fieldValue.toString()), entry.getKey().getRelationshipName()));
        } catch (URISyntaxException e) {
          log.error("Invalid destination urn: {}", fieldValue.toString(), e);
        }
      }
    }
    return Pair.of(edgesToAdd, relationshipTypesBeingAdded);
  }

  /**
   * Process snapshot and update graph index
   */
  private void updateGraphService(Urn urn, AspectSpec aspectSpec, RecordTemplate aspect) {
    Pair<List<Edge>, Set<String>> edgeAndRelationTypes =
        getEdgesAndRelationshipTypesFromAspect(urn, aspectSpec, aspect);

    final List<Edge> edgesToAdd = edgeAndRelationTypes.getFirst();
    final Set<String> relationshipTypesBeingAdded = edgeAndRelationTypes.getSecond();

    log.debug("Here's the relationship types found {}", relationshipTypesBeingAdded);
    if (relationshipTypesBeingAdded.size() > 0) {
      _graphService.removeEdgesFromNode(urn, new ArrayList<>(relationshipTypesBeingAdded),
          newRelationshipFilter(new Filter().setOr(new ConjunctiveCriterionArray()), RelationshipDirection.OUTGOING));
      edgesToAdd.forEach(edge -> _graphService.addEdge(edge));
    }
  }

  /**
   * Process snapshot and update search index
   */
  private void updateSearchService(String entityName, Urn urn, AspectSpec aspectSpec, RecordTemplate aspect, @Nullable String runId) {
    Optional<String> searchDocument;
    try {
      searchDocument = _searchDocumentTransformer.transformAspect(urn, aspect, aspectSpec, false);
    } catch (Exception e) {
      log.error("Error in getting documents from aspect: {} for aspect {}", e, aspectSpec.getName());
      return;
    }

    if (!searchDocument.isPresent()) {
      return;
    }

    Optional<String> docId = SearchUtils.getDocId(urn);

    if (!docId.isPresent()) {
      return;
    }

    _entitySearchService.upsertDocument(entityName, searchDocument.get(), docId.get());
  }

  /**
   * Process snapshot and update timseries index
   */
  private void updateTimeseriesFields(String entityType, String aspectName, Urn urn, RecordTemplate aspect,
      AspectSpec aspectSpec, SystemMetadata systemMetadata) {
    Map<String, JsonNode> documents;
    try {
      documents = TimeseriesAspectTransformer.transform(urn, aspect, aspectSpec, systemMetadata);
    } catch (JsonProcessingException e) {
      log.error("Failed to generate timeseries document from aspect: {}", e.toString());
      return;
    }
    documents.entrySet().forEach(document -> {
      _timeseriesAspectService.upsertDocument(entityType, aspectName, document.getKey(), document.getValue());
    });
  }

  private void updateSystemMetadata(SystemMetadata systemMetadata, Urn urn, AspectSpec aspectSpec, RecordTemplate aspect) {
    _systemMetadataService.insert(systemMetadata, urn.toString(), aspectSpec.getName());

    // If processing status aspect update all aspects for this urn to removed
    if (aspectSpec.getName().equals(Constants.STATUS_ASPECT_NAME)) {
      _systemMetadataService.setDocStatus(urn.toString(), ((Status) aspect).isRemoved());
    }
  }

  private void deleteSystemMetadata(Urn urn, AspectSpec aspectSpec, Boolean isKeyAspect) {
    if (isKeyAspect) {
      // Delete all aspects
      log.debug(String.format("Deleting all system metadata for urn: %s", urn));
      _systemMetadataService.deleteUrn(urn.toString());
    } else {
      // Delete all aspects from system metadata service
      log.debug(String.format("Deleting system metadata for urn: %s, aspect: %s", urn, aspectSpec.getName()));
      _systemMetadataService.deleteAspect(urn.toString(), aspectSpec.getName());
    }
  }

  private void deleteGraphData(Urn urn, AspectSpec aspectSpec, RecordTemplate aspect, Boolean isKeyAspect) {
    if (isKeyAspect) {
      _graphService.removeNode(urn);
      return;
    }

    Pair<List<Edge>, Set<String>> edgeAndRelationTypes =
        getEdgesAndRelationshipTypesFromAspect(urn, aspectSpec, aspect);

    final Set<String> relationshipTypesBeingAdded = edgeAndRelationTypes.getSecond();
    if (relationshipTypesBeingAdded.size() > 0) {
      _graphService.removeEdgesFromNode(urn, new ArrayList<>(relationshipTypesBeingAdded),
          createRelationshipFilter(new Filter().setOr(new ConjunctiveCriterionArray()), RelationshipDirection.OUTGOING));
    }
  }

  private void deleteSearchData(Urn urn, String entityName, AspectSpec aspectSpec, RecordTemplate aspect, Boolean isKeyAspect) {
      String docId;
      try {
        docId = URLEncoder.encode(urn.toString(), "UTF-8");
      } catch (UnsupportedEncodingException e) {
        log.error("Failed to encode the urn with error: {}", e.toString());
        return;
      }

      if (isKeyAspect) {
        _entitySearchService.deleteDocument(entityName, docId);
        return;
      }

      Optional<String> searchDocument;
      try {
        searchDocument = _searchDocumentTransformer.transformAspect(urn, aspect, aspectSpec, true); // TODO
      } catch (Exception e) {
        log.error("Error in getting documents from aspect: {} for aspect {}", e, aspectSpec.getName());
        return;
      }

      if (!searchDocument.isPresent()) {
        return;
      }

    _entitySearchService.upsertDocument(entityName, searchDocument.get(), docId);
  }
}