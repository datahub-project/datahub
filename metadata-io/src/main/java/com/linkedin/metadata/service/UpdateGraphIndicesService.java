package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.FORCE_INDEXING_KEY;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.UPSTREAM_LINEAGE_ASPECT_NAME;
import static com.linkedin.metadata.search.utils.QueryUtils.createRelationshipFilter;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.EdgeUrnType;
import com.linkedin.metadata.entity.SearchIndicesService;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.graph.EdgeDiff;
import com.linkedin.metadata.graph.GraphIndexUtils;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.key.DataPlatformKey;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

@Slf4j
public class UpdateGraphIndicesService implements SearchIndicesService {
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
    handleChangeEvents(opContext, java.util.Collections.singletonList(event));
  }

  @Override
  public void handleChangeEvents(
      @Nonnull OperationContext opContext, @Nonnull final Collection<MetadataChangeLog> events) {
    for (MetadataChangeLog event : events) {
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

  /** Process snapshot and update graph index */
  private void updateGraphService(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final RecordTemplate aspect,
      @Nonnull final MetadataChangeLog event) {
    Pair<List<Edge>, HashMap<Urn, Set<String>>> edgeAndRelationTypes =
        GraphIndexUtils.getEdgesAndRelationshipTypesFromAspect(
            urn, aspectSpec, aspect, event, true);

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
    EdgeDiff edgeDiff =
        GraphIndexUtils.computeAspectEdgeDiff(urn, aspectSpec, oldAspect, newAspect, event);

    // Edges to add
    final List<Edge> additiveDifference = edgeDiff.getEdgesToAdd();

    // Edges to remove
    final List<Edge> subtractiveDifference = edgeDiff.getEdgesToRemove();

    // Edges to update
    final List<Edge> mergedEdges = edgeDiff.getEdgesToUpdate();

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
          GraphIndexUtils.getEdgesAndRelationshipTypesFromAspect(
              urn, aspectSpec, aspect, event, true);

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
