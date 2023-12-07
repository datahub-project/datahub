package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.utils.QueryUtils.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.InputField;
import com.linkedin.common.InputFields;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.Edge;
import com.linkedin.metadata.graph.GraphIndexUtils;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.dgraph.DgraphGraphService;
import com.linkedin.metadata.key.SchemaFieldKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.SearchUtils;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.transformer.TimeseriesAspectTransformer;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;

@Slf4j
public class UpdateIndicesService {
  private static final String DOWNSTREAM_OF = "DownstreamOf";

  private final GraphService _graphService;
  private final EntitySearchService _entitySearchService;
  private final TimeseriesAspectService _timeseriesAspectService;
  private final SystemMetadataService _systemMetadataService;
  private final EntityRegistry _entityRegistry;
  private final SearchDocumentTransformer _searchDocumentTransformer;
  private final EntityIndexBuilders _entityIndexBuilders;

  @Value("${featureFlags.graphServiceDiffModeEnabled:true}")
  private boolean _graphDiffMode;

  @Value("${featureFlags.searchServiceDiffModeEnabled:true}")
  private boolean _searchDiffMode;

  private static final Set<ChangeType> UPDATE_CHANGE_TYPES =
      ImmutableSet.of(ChangeType.UPSERT, ChangeType.RESTATE, ChangeType.PATCH);

  @VisibleForTesting
  public void setGraphDiffMode(boolean graphDiffMode) {
    _graphDiffMode = graphDiffMode;
  }

  @VisibleForTesting
  public void setSearchDiffMode(boolean searchDiffMode) {
    _searchDiffMode = searchDiffMode;
  }

  public UpdateIndicesService(
      GraphService graphService,
      EntitySearchService entitySearchService,
      TimeseriesAspectService timeseriesAspectService,
      SystemMetadataService systemMetadataService,
      EntityRegistry entityRegistry,
      SearchDocumentTransformer searchDocumentTransformer,
      EntityIndexBuilders entityIndexBuilders) {
    _graphService = graphService;
    _entitySearchService = entitySearchService;
    _timeseriesAspectService = timeseriesAspectService;
    _systemMetadataService = systemMetadataService;
    _entityRegistry = entityRegistry;
    _searchDocumentTransformer = searchDocumentTransformer;
    _entityIndexBuilders = entityIndexBuilders;
  }

  public void handleChangeEvent(@Nonnull final MetadataChangeLog event) {
    try {
      if (UPDATE_CHANGE_TYPES.contains(event.getChangeType())) {
        handleUpdateChangeEvent(event);
      } else if (event.getChangeType() == ChangeType.DELETE) {
        handleDeleteChangeEvent(event);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This very important method processes {@link MetadataChangeLog} events that represent changes to
   * the Metadata Graph.
   *
   * <p>In particular, it handles updating the Search, Graph, Timeseries, and System Metadata stores
   * in response to a given change type to reflect the changes present in the new aspect.
   *
   * @param event the change event to be processed.
   */
  public void handleUpdateChangeEvent(@Nonnull final MetadataChangeLog event) throws IOException {

    final EntitySpec entitySpec = getEventEntitySpec(event);
    final Urn urn = EntityKeyUtils.getUrnFromLog(event, entitySpec.getKeyAspectSpec());

    if (!event.hasAspectName() || !event.hasAspect()) {
      log.error("Aspect or aspect name is missing. Skipping aspect processing...");
      return;
    }

    AspectSpec aspectSpec = entitySpec.getAspectSpec(event.getAspectName());
    if (aspectSpec == null) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve Aspect Spec for entity with name %s, aspect with name %s. Cannot update indices for MCL.",
              event.getEntityType(), event.getAspectName()));
    }

    RecordTemplate aspect =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(), event.getAspect().getContentType(), aspectSpec);
    GenericAspect previousAspectValue = event.getPreviousAspectValue();
    RecordTemplate previousAspect =
        previousAspectValue != null
            ? GenericRecordUtils.deserializeAspect(
                previousAspectValue.getValue(), previousAspectValue.getContentType(), aspectSpec)
            : null;

    // Step 0. If the aspect is timeseries, add to its timeseries index.
    if (aspectSpec.isTimeseries()) {
      updateTimeseriesFields(
          event.getEntityType(),
          event.getAspectName(),
          urn,
          aspect,
          aspectSpec,
          event.getSystemMetadata());
    } else {
      // Inject into the System Metadata Index when an aspect is non-timeseries only.
      // TODO: Verify whether timeseries aspects can be dropped into System Metadata as well
      // without impacting rollbacks.
      updateSystemMetadata(event.getSystemMetadata(), urn, aspectSpec, aspect);
    }

    // Step 1. For all aspects, attempt to update Search
    updateSearchService(
        entitySpec.getName(), urn, aspectSpec, aspect, event.getSystemMetadata(), previousAspect);

    // Step 2. For all aspects, attempt to update Graph
    SystemMetadata systemMetadata = event.getSystemMetadata();
    if (_graphDiffMode
        && !(_graphService instanceof DgraphGraphService)
        && (systemMetadata == null
            || systemMetadata.getProperties() == null
            || !Boolean.parseBoolean(systemMetadata.getProperties().get(FORCE_INDEXING_KEY)))) {
      updateGraphServiceDiff(urn, aspectSpec, previousAspect, aspect, event);
    } else {
      updateGraphService(urn, aspectSpec, aspect, event);
    }
  }

  /**
   * This very important method processes {@link MetadataChangeLog} deletion events to cleanup the
   * Metadata Graph when an aspect or entity is removed.
   *
   * <p>In particular, it handles updating the Search, Graph, Timeseries, and System Metadata stores
   * to reflect the deletion of a particular aspect.
   *
   * <p>Note that if an entity's key aspect is deleted, the entire entity will be purged from
   * search, graph, timeseries, etc.
   *
   * @param event the change event to be processed.
   */
  public void handleDeleteChangeEvent(@Nonnull final MetadataChangeLog event) {

    final EntitySpec entitySpec = getEventEntitySpec(event);
    final Urn urn = EntityKeyUtils.getUrnFromLog(event, entitySpec.getKeyAspectSpec());

    if (!event.hasAspectName() || !event.hasPreviousAspectValue()) {
      log.error("Previous aspect or aspect name is missing. Skipping aspect processing...");
      return;
    }

    AspectSpec aspectSpec = entitySpec.getAspectSpec(event.getAspectName());
    if (aspectSpec == null) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve Aspect Spec for entity with name %s, aspect with name %s. Cannot update indices for MCL.",
              event.getEntityType(), event.getAspectName()));
    }

    RecordTemplate aspect =
        GenericRecordUtils.deserializeAspect(
            event.getPreviousAspectValue().getValue(),
            event.getPreviousAspectValue().getContentType(),
            aspectSpec);
    Boolean isDeletingKey = event.getAspectName().equals(entitySpec.getKeyAspectName());

    if (!aspectSpec.isTimeseries()) {
      deleteSystemMetadata(urn, aspectSpec, isDeletingKey);
      deleteGraphData(urn, aspectSpec, aspect, isDeletingKey, event);
      deleteSearchData(
          _entitySearchService, urn, entitySpec.getName(), aspectSpec, aspect, isDeletingKey);
    }
  }

  // TODO: remove this method once we implement sourceOverride when creating graph edges
  private void updateFineGrainedEdgesAndRelationships(
      RecordTemplate aspect,
      List<Edge> edgesToAdd,
      HashMap<Urn, Set<String>> urnToRelationshipTypesBeingAdded) {
    UpstreamLineage upstreamLineage = new UpstreamLineage(aspect.data());
    if (upstreamLineage.getFineGrainedLineages() != null) {
      for (FineGrainedLineage fineGrainedLineage : upstreamLineage.getFineGrainedLineages()) {
        if (!fineGrainedLineage.hasDownstreams() || !fineGrainedLineage.hasUpstreams()) {
          break;
        }
        // for every downstream, create an edge with each of the upstreams
        for (Urn downstream : fineGrainedLineage.getDownstreams()) {
          for (Urn upstream : fineGrainedLineage.getUpstreams()) {
            // TODO: add edges uniformly across aspects
            edgesToAdd.add(
                new Edge(downstream, upstream, DOWNSTREAM_OF, null, null, null, null, null));
            Set<String> relationshipTypes =
                urnToRelationshipTypesBeingAdded.getOrDefault(downstream, new HashSet<>());
            relationshipTypes.add(DOWNSTREAM_OF);
            urnToRelationshipTypesBeingAdded.put(downstream, relationshipTypes);
          }
        }
      }
    }
  }

  private Urn generateSchemaFieldUrn(
      @Nonnull final String resourceUrn, @Nonnull final String fieldPath) {
    // we rely on schemaField fieldPaths to be encoded since we do that with fineGrainedLineage on
    // the ingestion side
    final String encodedFieldPath =
        fieldPath.replaceAll("\\(", "%28").replaceAll("\\)", "%29").replaceAll(",", "%2C");
    final SchemaFieldKey key =
        new SchemaFieldKey().setParent(UrnUtils.getUrn(resourceUrn)).setFieldPath(encodedFieldPath);
    return EntityKeyUtils.convertEntityKeyToUrn(key, Constants.SCHEMA_FIELD_ENTITY_NAME);
  }

  // TODO: remove this method once we implement sourceOverride and update inputFields aspect
  private void updateInputFieldEdgesAndRelationships(
      @Nonnull final Urn urn,
      @Nonnull final InputFields inputFields,
      @Nonnull final List<Edge> edgesToAdd,
      @Nonnull final HashMap<Urn, Set<String>> urnToRelationshipTypesBeingAdded) {
    if (inputFields.hasFields()) {
      for (final InputField field : inputFields.getFields()) {
        if (field.hasSchemaFieldUrn()
            && field.hasSchemaField()
            && field.getSchemaField().hasFieldPath()) {
          final Urn sourceFieldUrn =
              generateSchemaFieldUrn(urn.toString(), field.getSchemaField().getFieldPath());
          // TODO: add edges uniformly across aspects
          edgesToAdd.add(
              new Edge(
                  sourceFieldUrn,
                  field.getSchemaFieldUrn(),
                  DOWNSTREAM_OF,
                  null,
                  null,
                  null,
                  null,
                  null));
          final Set<String> relationshipTypes =
              urnToRelationshipTypesBeingAdded.getOrDefault(sourceFieldUrn, new HashSet<>());
          relationshipTypes.add(DOWNSTREAM_OF);
          urnToRelationshipTypesBeingAdded.put(sourceFieldUrn, relationshipTypes);
        }
      }
    }
  }

  private Pair<List<Edge>, HashMap<Urn, Set<String>>> getEdgesAndRelationshipTypesFromAspect(
      @Nonnull final Urn urn,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final RecordTemplate aspect,
      @Nonnull final MetadataChangeLog event,
      final boolean isNewAspectVersion) {
    final List<Edge> edgesToAdd = new ArrayList<>();
    final HashMap<Urn, Set<String>> urnToRelationshipTypesBeingAdded = new HashMap<>();

    // we need to manually set schemaField <-> schemaField edges for fineGrainedLineage and
    // inputFields
    // since @Relationship only links between the parent entity urn and something else.
    if (aspectSpec.getName().equals(Constants.UPSTREAM_LINEAGE_ASPECT_NAME)) {
      updateFineGrainedEdgesAndRelationships(aspect, edgesToAdd, urnToRelationshipTypesBeingAdded);
    }
    if (aspectSpec.getName().equals(Constants.INPUT_FIELDS_ASPECT_NAME)) {
      final InputFields inputFields = new InputFields(aspect.data());
      updateInputFieldEdgesAndRelationships(
          urn, inputFields, edgesToAdd, urnToRelationshipTypesBeingAdded);
    }

    Map<RelationshipFieldSpec, List<Object>> extractedFields =
        FieldExtractor.extractFields(aspect, aspectSpec.getRelationshipFieldSpecs());

    for (Map.Entry<RelationshipFieldSpec, List<Object>> entry : extractedFields.entrySet()) {
      Set<String> relationshipTypes =
          urnToRelationshipTypesBeingAdded.getOrDefault(urn, new HashSet<>());
      relationshipTypes.add(entry.getKey().getRelationshipName());
      urnToRelationshipTypesBeingAdded.put(urn, relationshipTypes);
      final List<Edge> newEdges =
          GraphIndexUtils.extractGraphEdges(entry, aspect, urn, event, isNewAspectVersion);
      edgesToAdd.addAll(newEdges);
    }
    return Pair.of(edgesToAdd, urnToRelationshipTypesBeingAdded);
  }

  /** Process snapshot and update graph index */
  private void updateGraphService(
      @Nonnull final Urn urn,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final RecordTemplate aspect,
      @Nonnull final MetadataChangeLog event) {
    Pair<List<Edge>, HashMap<Urn, Set<String>>> edgeAndRelationTypes =
        getEdgesAndRelationshipTypesFromAspect(urn, aspectSpec, aspect, event, true);

    final List<Edge> edgesToAdd = edgeAndRelationTypes.getFirst();
    final HashMap<Urn, Set<String>> urnToRelationshipTypesBeingAdded =
        edgeAndRelationTypes.getSecond();

    log.debug("Here's the relationship types found {}", urnToRelationshipTypesBeingAdded);
    if (urnToRelationshipTypesBeingAdded.size() > 0) {
      for (Map.Entry<Urn, Set<String>> entry : urnToRelationshipTypesBeingAdded.entrySet()) {
        _graphService.removeEdgesFromNode(
            entry.getKey(),
            new ArrayList<>(entry.getValue()),
            newRelationshipFilter(
                new Filter().setOr(new ConjunctiveCriterionArray()),
                RelationshipDirection.OUTGOING));
      }
      edgesToAdd.forEach(_graphService::addEdge);
    }
  }

  private void updateGraphServiceDiff(
      @Nonnull final Urn urn,
      @Nonnull final AspectSpec aspectSpec,
      @Nullable final RecordTemplate oldAspect,
      @Nonnull final RecordTemplate newAspect,
      @Nonnull final MetadataChangeLog event) {
    Pair<List<Edge>, HashMap<Urn, Set<String>>> oldEdgeAndRelationTypes = null;
    if (oldAspect != null) {
      oldEdgeAndRelationTypes =
          getEdgesAndRelationshipTypesFromAspect(urn, aspectSpec, oldAspect, event, false);
    }

    final List<Edge> oldEdges =
        oldEdgeAndRelationTypes != null
            ? oldEdgeAndRelationTypes.getFirst()
            : Collections.emptyList();
    final Set<Edge> oldEdgeSet = new HashSet<>(oldEdges);

    Pair<List<Edge>, HashMap<Urn, Set<String>>> newEdgeAndRelationTypes =
        getEdgesAndRelationshipTypesFromAspect(urn, aspectSpec, newAspect, event, true);

    final List<Edge> newEdges = newEdgeAndRelationTypes.getFirst();
    final Set<Edge> newEdgeSet = new HashSet<>(newEdges);

    // Edges to add
    final List<Edge> additiveDifference =
        newEdgeSet.stream().filter(edge -> !oldEdgeSet.contains(edge)).collect(Collectors.toList());

    // Edges to remove
    final List<Edge> subtractiveDifference =
        oldEdgeSet.stream().filter(edge -> !newEdgeSet.contains(edge)).collect(Collectors.toList());

    // Edges to update
    final List<Edge> mergedEdges = getMergedEdges(oldEdgeSet, newEdgeSet);

    // Remove any old edges that no longer exist first
    if (subtractiveDifference.size() > 0) {
      log.debug("Removing edges: {}", subtractiveDifference);
      subtractiveDifference.forEach(_graphService::removeEdge);
    }

    // Then add new edges
    if (additiveDifference.size() > 0) {
      log.debug("Adding edges: {}", additiveDifference);
      additiveDifference.forEach(_graphService::addEdge);
    }

    // Then update existing edges
    if (mergedEdges.size() > 0) {
      log.debug("Updating edges: {}", mergedEdges);
      mergedEdges.forEach(_graphService::upsertEdge);
    }
  }

  private static List<Edge> getMergedEdges(final Set<Edge> oldEdgeSet, final Set<Edge> newEdgeSet) {
    final Map<Integer, com.linkedin.metadata.graph.Edge> oldEdgesMap =
        oldEdgeSet.stream()
            .map(edge -> Pair.of(edge.hashCode(), edge))
            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));

    final List<com.linkedin.metadata.graph.Edge> mergedEdges = new ArrayList<>();
    if (!oldEdgesMap.isEmpty()) {
      for (com.linkedin.metadata.graph.Edge newEdge : newEdgeSet) {
        if (oldEdgesMap.containsKey(newEdge.hashCode())) {
          final com.linkedin.metadata.graph.Edge oldEdge = oldEdgesMap.get(newEdge.hashCode());
          final com.linkedin.metadata.graph.Edge mergedEdge =
              GraphIndexUtils.mergeEdges(oldEdge, newEdge);
          mergedEdges.add(mergedEdge);
        }
      }
    }

    return mergedEdges;
  }

  /** Process snapshot and update search index */
  private void updateSearchService(
      String entityName,
      Urn urn,
      AspectSpec aspectSpec,
      RecordTemplate aspect,
      @Nullable SystemMetadata systemMetadata,
      @Nullable RecordTemplate previousAspect) {
    Optional<String> searchDocument;
    Optional<String> previousSearchDocument = Optional.empty();
    try {
      searchDocument = _searchDocumentTransformer.transformAspect(urn, aspect, aspectSpec, false);
    } catch (Exception e) {
      log.error(
          "Error in getting documents from aspect: {} for aspect {}", e, aspectSpec.getName());
      return;
    }

    if (!searchDocument.isPresent()) {
      return;
    }

    Optional<String> docId = SearchUtils.getDocId(urn);

    if (!docId.isPresent()) {
      return;
    }

    String searchDocumentValue = searchDocument.get();
    if (_searchDiffMode
        && (systemMetadata == null
            || systemMetadata.getProperties() == null
            || !Boolean.parseBoolean(systemMetadata.getProperties().get(FORCE_INDEXING_KEY)))) {
      if (previousAspect != null) {
        try {
          previousSearchDocument =
              _searchDocumentTransformer.transformAspect(urn, previousAspect, aspectSpec, false);
        } catch (Exception e) {
          log.error(
              "Error in getting documents from previous aspect state: {} for aspect {}, continuing without diffing.",
              e,
              aspectSpec.getName());
        }
      }

      if (previousSearchDocument.isPresent()) {
        String previousSearchDocumentValue = previousSearchDocument.get();
        if (searchDocumentValue.equals(previousSearchDocumentValue)) {
          // No changes to search document, skip writing no-op update
          return;
        }
      }
    }

    _entitySearchService.upsertDocument(entityName, searchDocument.get(), docId.get());
  }

  /** Process snapshot and update time-series index */
  private void updateTimeseriesFields(
      String entityType,
      String aspectName,
      Urn urn,
      RecordTemplate aspect,
      AspectSpec aspectSpec,
      SystemMetadata systemMetadata) {
    Map<String, JsonNode> documents;
    try {
      documents = TimeseriesAspectTransformer.transform(urn, aspect, aspectSpec, systemMetadata);
    } catch (JsonProcessingException e) {
      log.error("Failed to generate timeseries document from aspect: {}", e.toString());
      return;
    }
    documents
        .entrySet()
        .forEach(
            document -> {
              _timeseriesAspectService.upsertDocument(
                  entityType, aspectName, document.getKey(), document.getValue());
            });
  }

  private void updateSystemMetadata(
      SystemMetadata systemMetadata, Urn urn, AspectSpec aspectSpec, RecordTemplate aspect) {
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
      log.debug(
          String.format(
              "Deleting system metadata for urn: %s, aspect: %s", urn, aspectSpec.getName()));
      _systemMetadataService.deleteAspect(urn.toString(), aspectSpec.getName());
    }
  }

  private void deleteGraphData(
      @Nonnull final Urn urn,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final RecordTemplate aspect,
      @Nonnull final Boolean isKeyAspect,
      @Nonnull final MetadataChangeLog event) {
    if (isKeyAspect) {
      _graphService.removeNode(urn);
      return;
    }

    Pair<List<Edge>, HashMap<Urn, Set<String>>> edgeAndRelationTypes =
        getEdgesAndRelationshipTypesFromAspect(urn, aspectSpec, aspect, event, true);

    final HashMap<Urn, Set<String>> urnToRelationshipTypesBeingAdded =
        edgeAndRelationTypes.getSecond();
    if (urnToRelationshipTypesBeingAdded.size() > 0) {
      for (Map.Entry<Urn, Set<String>> entry : urnToRelationshipTypesBeingAdded.entrySet()) {
        _graphService.removeEdgesFromNode(
            entry.getKey(),
            new ArrayList<>(entry.getValue()),
            createRelationshipFilter(
                new Filter().setOr(new ConjunctiveCriterionArray()),
                RelationshipDirection.OUTGOING));
      }
    }
  }

  private void deleteSearchData(
      EntitySearchService entitySearchService,
      Urn urn,
      String entityName,
      AspectSpec aspectSpec,
      RecordTemplate aspect,
      Boolean isKeyAspect) {
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
      searchDocument =
          _searchDocumentTransformer.transformAspect(urn, aspect, aspectSpec, true); // TODO
    } catch (Exception e) {
      log.error(
          "Error in getting documents from aspect: {} for aspect {}", e, aspectSpec.getName());
      return;
    }

    if (!searchDocument.isPresent()) {
      return;
    }

    _entitySearchService.upsertDocument(entityName, searchDocument.get(), docId);
  }

  private EntitySpec getEventEntitySpec(@Nonnull final MetadataChangeLog event) {
    try {
      return _entityRegistry.getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve Entity Spec for entity with name %s. Cannot update indices for MCL.",
              event.getEntityType()));
    }
  }

  /**
   * Allow internal use of the system entity client. Solves recursive dependencies between the
   * UpdateIndicesService and the SystemJavaEntityClient
   *
   * @param systemEntityClient system entity client
   */
  public void setSystemEntityClient(SystemEntityClient systemEntityClient) {
    _searchDocumentTransformer.setEntityClient(systemEntityClient);
  }
}
