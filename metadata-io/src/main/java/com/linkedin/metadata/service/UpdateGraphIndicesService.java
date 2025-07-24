package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.FORCE_INDEXING_KEY;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.UPSTREAM_LINEAGE_ASPECT_NAME;
import static com.linkedin.metadata.search.utils.QueryUtils.createRelationshipFilter;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.InputField;
import com.linkedin.common.InputFields;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.EdgeUrnType;
import com.linkedin.metadata.entity.SearchIndicesService;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.graph.GraphIndexUtils;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.key.DataPlatformKey;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

@Slf4j
public class UpdateGraphIndicesService implements SearchIndicesService {
  private static final String DOWNSTREAM_OF = "DownstreamOf";
  private static final String GRAPH_DIFF_MODE_REMOVE_METRIC = "diff_remove_edge";
  private static final String GRAPH_DIFF_MODE_ADD_METRIC = "diff_add_edge";
  private static final String GRAPH_DIFF_MODE_UPDATE_METRIC = "diff_update_edge";
  private List<String> fineGrainedLineageNotAllowedForPlatforms;
  private final String FINE_GRAINED_LINEAGE_PATH = "/fineGrainedLineages/*/upstreams/*";

  public static UpdateGraphIndicesService withService(GraphService graphService) {
    return new UpdateGraphIndicesService(graphService);
  }

  private final GraphService graphService;

  @Getter private final boolean graphStatusEnabled;

  @Getter @Setter @VisibleForTesting private boolean graphDiffMode;

  @VisibleForTesting
  public void setFineGrainedLineageNotAllowedForPlatforms(List<String> platforms) {
    this.fineGrainedLineageNotAllowedForPlatforms = platforms;
  }

  private static final Set<ChangeType> UPDATE_CHANGE_TYPES =
      ImmutableSet.of(
          ChangeType.CREATE,
          ChangeType.CREATE_ENTITY,
          ChangeType.UPSERT,
          ChangeType.RESTATE,
          ChangeType.PATCH);

  public UpdateGraphIndicesService(GraphService graphService) {
    this(graphService, true, true, Collections.emptyList());
  }

  public UpdateGraphIndicesService(
      GraphService graphService,
      boolean graphDiffMode,
      boolean graphStatusEnabled,
      List<String> fineGrainedLineageNotAllowedForPlatforms) {
    this.graphService = graphService;
    this.graphDiffMode = graphDiffMode;
    this.graphStatusEnabled = graphStatusEnabled;
    this.fineGrainedLineageNotAllowedForPlatforms = fineGrainedLineageNotAllowedForPlatforms;
  }

  @Override
  public void handleChangeEvent(
      @Nonnull OperationContext opContext, @Nonnull final MetadataChangeLog event) {
    try {
      MCLItemImpl mclItem = MCLItemImpl.builder().build(event, opContext.getAspectRetriever());

      if (UPDATE_CHANGE_TYPES.contains(event.getChangeType())) {
        handleUpdateChangeEvent(opContext, mclItem);

        if (graphStatusEnabled && mclItem.getAspectName().equals(STATUS_ASPECT_NAME)) {
          handleStatusUpdateChangeEvent(opContext, mclItem);
        }
      } else if (event.getChangeType() == ChangeType.DELETE) {
        handleDeleteChangeEvent(opContext, mclItem);

        if (graphStatusEnabled && mclItem.getAspectName().equals(STATUS_ASPECT_NAME)) {
          handleStatusUpdateChangeEvent(opContext, mclItem);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void handleStatusUpdateChangeEvent(
      @Nonnull final OperationContext opContext, @Nonnull final MCLItem item) {
    final Boolean removed;
    if (ChangeType.DELETE.equals(item.getChangeType())) {
      removed = false;
    } else if (ChangeType.RESTATE.equals(item.getChangeType())
        || item.getPreviousRecordTemplate() == null
        || !item.getPreviousAspect(Status.class).equals(item.getAspect(Status.class))) {
      removed = item.getAspect(Status.class).isRemoved();
    } else {
      removed = null;
    }

    if (removed != null) {
      graphService.setEdgeStatus(item.getUrn(), removed, EdgeUrnType.values());
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
  private void handleUpdateChangeEvent(
      @Nonnull final OperationContext opContext, @Nonnull final MCLItem event) throws IOException {

    final AspectSpec aspectSpec = event.getAspectSpec();
    final Urn urn = event.getUrn();

    RecordTemplate aspect = event.getRecordTemplate();
    RecordTemplate previousAspect = event.getPreviousRecordTemplate();

    // For all aspects, attempt to update Graph
    SystemMetadata systemMetadata = event.getSystemMetadata();
    if (graphDiffMode
        && (systemMetadata == null
            || systemMetadata.getProperties() == null
            || !Boolean.parseBoolean(systemMetadata.getProperties().get(FORCE_INDEXING_KEY)))) {
      updateGraphServiceDiff(
          opContext, urn, aspectSpec, previousAspect, aspect, event.getMetadataChangeLog());
    } else {
      updateGraphService(opContext, urn, aspectSpec, aspect, event.getMetadataChangeLog());
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
  private void handleDeleteChangeEvent(
      @Nonnull final OperationContext opContext, @Nonnull final MCLItem event) {

    final EntitySpec entitySpec = event.getEntitySpec();
    final Urn urn = event.getUrn();

    AspectSpec aspectSpec = entitySpec.getAspectSpec(event.getAspectName());
    if (aspectSpec == null) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve Aspect Spec for entity with name %s, aspect with name %s. Cannot update indices for MCL.",
              urn.getEntityType(), event.getAspectName()));
    }

    final RecordTemplate aspect =
        event.getPreviousRecordTemplate() != null
            ? event.getPreviousRecordTemplate()
            : event.getRecordTemplate();
    Boolean isDeletingKey = event.getAspectName().equals(entitySpec.getKeyAspectName());

    if (!aspectSpec.isTimeseries()) {
      deleteGraphData(
          opContext, urn, aspectSpec, aspect, isDeletingKey, event.getMetadataChangeLog());
    }
  }

  // TODO: remove this method once we implement sourceOverride when creating graph edges
  private void updateFineGrainedEdgesAndRelationships(
      Urn entity,
      FineGrainedLineageArray fineGrainedLineageArray,
      List<Edge> edgesToAdd,
      HashMap<Urn, Set<String>> urnToRelationshipTypesBeingAdded,
      final EntityRegistry entityRegistry) {
    if (fineGrainedLineageArray != null) {
      for (FineGrainedLineage fineGrainedLineage : fineGrainedLineageArray) {
        if (!fineGrainedLineage.hasDownstreams() || !fineGrainedLineage.hasUpstreams()) {
          break;
        }
        // Fine grained lineage array is present either on datajob (datajob input/output) or dataset
        // We set the datajob as the viaEntity in scenario 1, and the query (if present) as the
        // viaEntity in scenario 2
        Urn viaEntity =
            entity.getEntityType().equals("dataJob") ? entity : fineGrainedLineage.getQuery();
        // for every downstream, create an edge with each of the upstreams
        for (Urn downstream : fineGrainedLineage.getDownstreams()) {
          for (Urn upstream : fineGrainedLineage.getUpstreams()) {
            // TODO: add edges uniformly across aspects

            // restrict the creation of schemafield nodes and their relationships
            // especially for platforms like hdfs
            if (isFineGrainedLineageNotAllowedForPlatforms(downstream, upstream, entityRegistry)) {
              log.debug(
                  "Skipping fine grained lineage for downstream {} and upstream {}",
                  downstream,
                  upstream);
              continue;
            }

            edgesToAdd.add(
                new Edge(
                    downstream,
                    upstream,
                    DOWNSTREAM_OF,
                    null,
                    null,
                    null,
                    null,
                    null,
                    entity,
                    viaEntity));
            Set<String> relationshipTypes =
                urnToRelationshipTypesBeingAdded.getOrDefault(downstream, new HashSet<>());
            relationshipTypes.add(DOWNSTREAM_OF);
            urnToRelationshipTypesBeingAdded.put(downstream, relationshipTypes);
          }
        }
      }
    }
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
              SchemaFieldUtils.generateSchemaFieldUrn(urn, field.getSchemaField().getFieldPath());
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
      final boolean isNewAspectVersion,
      final EntityRegistry entityRegistry) {
    final List<Edge> edges = new ArrayList<>();
    final HashMap<Urn, Set<String>> urnToRelationshipTypes = new HashMap<>();

    // we need to manually set schemaField <-> schemaField edges for fineGrainedLineage and
    // inputFields
    // since @Relationship only links between the parent entity urn and something else.
    if (aspectSpec.getName().equals(UPSTREAM_LINEAGE_ASPECT_NAME)) {
      UpstreamLineage upstreamLineage = new UpstreamLineage(aspect.data());
      updateFineGrainedEdgesAndRelationships(
          urn,
          upstreamLineage.getFineGrainedLineages(),
          edges,
          urnToRelationshipTypes,
          entityRegistry);
    } else if (aspectSpec.getName().equals(Constants.INPUT_FIELDS_ASPECT_NAME)) {
      final InputFields inputFields = new InputFields(aspect.data());
      updateInputFieldEdgesAndRelationships(urn, inputFields, edges, urnToRelationshipTypes);
    } else if (aspectSpec.getName().equals(Constants.DATA_JOB_INPUT_OUTPUT_ASPECT_NAME)) {
      DataJobInputOutput dataJobInputOutput = new DataJobInputOutput(aspect.data());
      updateFineGrainedEdgesAndRelationships(
          urn,
          dataJobInputOutput.getFineGrainedLineages(),
          edges,
          urnToRelationshipTypes,
          entityRegistry);
    }

    Map<RelationshipFieldSpec, List<Object>> extractedFields =
        FieldExtractor.extractFields(aspect, aspectSpec.getRelationshipFieldSpecs(), true);

    // restrict the creation of schema field nodes and their relationships especially for
    // platforms like hdfs
    removeFineGrainedLineageForNotAllowedPlatforms(extractedFields, aspectSpec, entityRegistry);

    for (Map.Entry<RelationshipFieldSpec, List<Object>> entry : extractedFields.entrySet()) {
      Set<String> relationshipTypes = urnToRelationshipTypes.getOrDefault(urn, new HashSet<>());
      relationshipTypes.add(entry.getKey().getRelationshipName());
      urnToRelationshipTypes.put(urn, relationshipTypes);
      final List<Edge> newEdges =
          GraphIndexUtils.extractGraphEdges(entry, aspect, urn, event, isNewAspectVersion);
      edges.addAll(newEdges);
    }
    return Pair.of(edges, urnToRelationshipTypes);
  }

  /** Process snapshot and update graph index */
  private void updateGraphService(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final RecordTemplate aspect,
      @Nonnull final MetadataChangeLog event) {
    Pair<List<Edge>, HashMap<Urn, Set<String>>> edgeAndRelationTypes =
        getEdgesAndRelationshipTypesFromAspect(
            urn, aspectSpec, aspect, event, true, opContext.getEntityRegistry());

    final List<Edge> edgesToAdd = edgeAndRelationTypes.getFirst();
    final HashMap<Urn, Set<String>> urnToRelationshipTypesBeingAdded =
        edgeAndRelationTypes.getSecond();

    log.debug("Here's the relationship types found {}", urnToRelationshipTypesBeingAdded);
    if (!urnToRelationshipTypesBeingAdded.isEmpty()) {
      for (Map.Entry<Urn, Set<String>> entry : urnToRelationshipTypesBeingAdded.entrySet()) {
        graphService.removeEdgesFromNode(
            opContext,
            entry.getKey(),
            entry.getValue(),
            newRelationshipFilter(
                new Filter().setOr(new ConjunctiveCriterionArray()),
                RelationshipDirection.OUTGOING));
      }
      edgesToAdd.forEach(graphService::addEdge);
    }
  }

  private void updateGraphServiceDiff(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final AspectSpec aspectSpec,
      @Nullable final RecordTemplate oldAspect,
      @Nonnull final RecordTemplate newAspect,
      @Nonnull final MetadataChangeLog event) {
    Pair<List<Edge>, HashMap<Urn, Set<String>>> oldEdgeAndRelationTypes = null;
    if (oldAspect != null) {
      oldEdgeAndRelationTypes =
          getEdgesAndRelationshipTypesFromAspect(
              urn, aspectSpec, oldAspect, event, false, opContext.getEntityRegistry());
    }

    final List<Edge> oldEdges =
        oldEdgeAndRelationTypes != null
            ? oldEdgeAndRelationTypes.getFirst()
            : Collections.emptyList();
    final Set<Edge> oldEdgeSet = new HashSet<>(oldEdges);

    Pair<List<Edge>, HashMap<Urn, Set<String>>> newEdgeAndRelationTypes =
        getEdgesAndRelationshipTypesFromAspect(
            urn, aspectSpec, newAspect, event, true, opContext.getEntityRegistry());

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
    if (!subtractiveDifference.isEmpty()) {
      log.debug("Removing edges: {}", subtractiveDifference);
      subtractiveDifference.forEach(graphService::removeEdge);
      opContext
          .getMetricUtils()
          .ifPresent(
              metricUtils ->
                  metricUtils.increment(
                      this.getClass(),
                      GRAPH_DIFF_MODE_REMOVE_METRIC,
                      subtractiveDifference.size()));
    }

    // Then add new edges
    if (!additiveDifference.isEmpty()) {
      log.debug("Adding edges: {}", additiveDifference);
      additiveDifference.forEach(graphService::addEdge);
      opContext
          .getMetricUtils()
          .ifPresent(
              metricUtils ->
                  metricUtils.increment(
                      this.getClass(), GRAPH_DIFF_MODE_ADD_METRIC, additiveDifference.size()));
    }

    // Then update existing edges
    if (!mergedEdges.isEmpty()) {
      log.debug("Updating edges: {}", mergedEdges);
      mergedEdges.forEach(graphService::upsertEdge);
      opContext
          .getMetricUtils()
          .ifPresent(
              metricUtils ->
                  metricUtils.increment(
                      this.getClass(), GRAPH_DIFF_MODE_UPDATE_METRIC, mergedEdges.size()));
    }
  }

  private static List<Edge> getMergedEdges(final Set<Edge> oldEdgeSet, final Set<Edge> newEdgeSet) {
    final Map<Integer, Edge> oldEdgesMap =
        oldEdgeSet.stream()
            .map(edge -> Pair.of(edge.hashCode(), edge))
            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));

    final List<Edge> mergedEdges = new ArrayList<>();
    if (!oldEdgesMap.isEmpty()) {
      for (Edge newEdge : newEdgeSet) {
        if (oldEdgesMap.containsKey(newEdge.hashCode())) {
          final Edge oldEdge = oldEdgesMap.get(newEdge.hashCode());
          final Edge mergedEdge = GraphIndexUtils.mergeEdges(oldEdge, newEdge);
          mergedEdges.add(mergedEdge);
        }
      }
    }

    return mergedEdges;
  }

  private void deleteGraphData(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final AspectSpec aspectSpec,
      @Nullable final RecordTemplate aspect,
      @Nonnull final Boolean isKeyAspect,
      @Nonnull final MetadataChangeLog event) {
    if (isKeyAspect) {
      graphService.removeNode(opContext, urn);
      return;
    }

    if (aspect != null) {
      Pair<List<Edge>, HashMap<Urn, Set<String>>> edgeAndRelationTypes =
          getEdgesAndRelationshipTypesFromAspect(
              urn, aspectSpec, aspect, event, true, opContext.getEntityRegistry());

      final HashMap<Urn, Set<String>> urnToRelationshipTypesBeingRemoved =
          edgeAndRelationTypes.getSecond();
      if (!urnToRelationshipTypesBeingRemoved.isEmpty()) {
        for (Map.Entry<Urn, Set<String>> entry : urnToRelationshipTypesBeingRemoved.entrySet()) {
          graphService.removeEdgesFromNode(
              opContext,
              entry.getKey(),
              entry.getValue(),
              createRelationshipFilter(
                  new Filter().setOr(new ConjunctiveCriterionArray()),
                  RelationshipDirection.OUTGOING));
        }
      }
    } else {
      log.warn(
          "Insufficient information to perform graph delete. Missing deleted aspect {} for entity {}",
          aspectSpec.getName(),
          urn);
    }
  }

  private void removeFineGrainedLineageForNotAllowedPlatforms(
      Map<RelationshipFieldSpec, List<Object>> extractedFields,
      AspectSpec aspectSpec,
      EntityRegistry entityRegistry) {
    if (!aspectSpec.getName().equals(UPSTREAM_LINEAGE_ASPECT_NAME)) {
      return;
    }
    RelationshipFieldSpec fineGrainedLineageFieldSpec =
        aspectSpec.getRelationshipFieldSpecMap().get(FINE_GRAINED_LINEAGE_PATH);
    List<Object> fineGrainedLineageUrnsList = extractedFields.get(fineGrainedLineageFieldSpec);
    fineGrainedLineageUrnsList.removeIf(
        fineGrainedLineageUrn -> {
          if (fineGrainedLineageUrn instanceof Urn) {
            Urn upstreamSchemaFieldUrn = (Urn) fineGrainedLineageUrn;
            return isFineGrainedLineageNotAllowedForPlatforms(
                null, upstreamSchemaFieldUrn, entityRegistry);
          }
          return false;
        });
  }

  private boolean isFineGrainedLineageNotAllowedForPlatforms(
      Urn downstream, Urn upstream, EntityRegistry entityRegistry) {
    return !CollectionUtils.isEmpty(fineGrainedLineageNotAllowedForPlatforms)
        && ((Objects.nonNull(downstream)
                && downstream.getEntityType().equals(SCHEMA_FIELD_ENTITY_NAME)
                && fineGrainedLineageNotAllowedForPlatforms.contains(
                    getDatasetPlatformName(entityRegistry, downstream.getIdAsUrn())))
            || (Objects.nonNull(upstream)
                && upstream.getEntityType().equals(SCHEMA_FIELD_ENTITY_NAME)
                && fineGrainedLineageNotAllowedForPlatforms.contains(
                    getDatasetPlatformName(entityRegistry, upstream.getIdAsUrn()))));
  }

  private String getDatasetPlatformName(EntityRegistry entityRegistry, Urn datasetUrn) {
    DatasetKey dsKey =
        (DatasetKey)
            EntityKeyUtils.convertUrnToEntityKey(
                datasetUrn,
                entityRegistry.getEntitySpec(datasetUrn.getEntityType()).getKeyAspectSpec());
    DataPlatformKey dpKey =
        (DataPlatformKey)
            EntityKeyUtils.convertUrnToEntityKey(
                dsKey.getPlatform(),
                entityRegistry
                    .getEntitySpec(dsKey.getPlatform().getEntityType())
                    .getKeyAspectSpec());

    return dpKey.getPlatformName();
  }
}
