package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.FORCE_INDEXING_KEY;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.search.utils.QueryUtils.createRelationshipFilter;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;
import static com.linkedin.metadata.utils.UnknownDataHandlings.handling;

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
import com.linkedin.metadata.graph.elastic.utils.GraphQueryConstants;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
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
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateGraphIndicesService implements SearchIndicesService {
  private static final String GRAPH_DIFF_MODE_REMOVE_METRIC = "diff_remove_edge";
  private static final String GRAPH_DIFF_MODE_ADD_METRIC = "diff_add_edge";
  private static final String GRAPH_DIFF_MODE_UPDATE_METRIC = "diff_update_edge";
  private List<String> fineGrainedLineageNotAllowedForPlatforms;

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
      MCLItemImpl mclItem = MCLItemImpl.builder().build(event, opContext.getAspectRetriever());
      handleItem(opContext, mclItem, mclItem.getPreviousRecordTemplate(), true);
    }
  }

  /**
   * Updates the graph for an event built without its previous value, because a newer version wrote
   * that value and this version can't validate it (e.g. an enum symbol only the newer version
   * defines). {@code unvalidatedPrevious} is that value as deserialized here, or null if it can't
   * be; only its urns are read. An upsert diffs against it, or replaces the edges of the new
   * value's relationship types when it is null. A delete removes its edges. As for any upsert, the
   * edges to entity types this version doesn't know are left as they are (see {@link
   * #handleUpdateChangeEvent}).
   */
  public void handleChangeEventWithUnvalidatedPrevious(
      @Nonnull OperationContext opContext,
      @Nonnull final MetadataChangeLog event,
      @Nullable final RecordTemplate unvalidatedPrevious) {
    MCLItemImpl mclItem = MCLItemImpl.builder().build(event, opContext.getAspectRetriever());
    handleItem(opContext, mclItem, unvalidatedPrevious, unvalidatedPrevious != null);
  }

  private void handleItem(
      @Nonnull final OperationContext opContext,
      @Nonnull final MCLItem mclItem,
      @Nullable final RecordTemplate previousAspect,
      final boolean diffable) {
    try {
      if (UPDATE_CHANGE_TYPES.contains(mclItem.getChangeType())) {
        handleUpdateChangeEvent(opContext, mclItem, previousAspect, diffable);

        if (graphStatusEnabled && mclItem.getAspectName().equals(STATUS_ASPECT_NAME)) {
          handleStatusUpdateChangeEvent(opContext, mclItem);
        }
      } else if (mclItem.getChangeType() == ChangeType.DELETE) {
        handleDeleteChangeEvent(opContext, mclItem, previousAspect);

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
      graphService.setEdgeStatus(opContext, item.getUrn(), removed, EdgeUrnType.values());
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
   * @param previousAspect the value the event replaces, to diff against
   * @param diffable false when there is no previous value to diff against, so the edges of the new
   *     value's relationship types are replaced even in diff mode
   */
  private void handleUpdateChangeEvent(
      @Nonnull final OperationContext opContext,
      @Nonnull final MCLItem event,
      @Nullable final RecordTemplate previousAspect,
      final boolean diffable)
      throws IOException {

    final AspectSpec aspectSpec = event.getAspectSpec();
    final Urn urn = event.getUrn();

    RecordTemplate aspect = event.getRecordTemplate();

    // With entity tolerance on, edges to entity types this version doesn't know (written by a
    // newer version) are left as they are (see knownDestinations). Building the item removed the
    // references to those types, whole list elements with them, so for an aspect whose MCL still
    // holds such references the graph is computed from the MCL's value instead: the known edges
    // of those elements are kept, added and removed like any other. Restore indices sends values
    // already stripped on read, so the edges to unknown types are kept even when the MCL holds
    // none of them.
    final EntityRegistry registry = opContext.getEntityRegistry();
    final boolean leaveUnknownDestinations = handling().toleratesUnknownEntityAspect();
    RecordTemplate graphAspect = aspect;
    RecordTemplate graphPrevious = previousAspect;
    if (leaveUnknownDestinations) {
      final RecordTemplate storedAspect =
          asStored(event.getMetadataChangeLog().getAspect(), aspectSpec, aspect);
      final RecordTemplate storedPrevious =
          event.getMetadataChangeLog().hasPreviousAspectValue()
              ? asStored(
                  event.getMetadataChangeLog().getPreviousAspectValue(), aspectSpec, previousAspect)
              : previousAspect;
      if (referencesUnknownEntityType(storedAspect, registry)
          || referencesUnknownEntityType(storedPrevious, registry)) {
        graphAspect = storedAspect;
        graphPrevious = storedPrevious;
      }
    }

    // For all aspects, attempt to update Graph
    SystemMetadata systemMetadata = event.getSystemMetadata();
    if (graphDiffMode
        && diffable
        && (systemMetadata == null
            || systemMetadata.getProperties() == null
            || !Boolean.parseBoolean(systemMetadata.getProperties().get(FORCE_INDEXING_KEY)))) {
      updateGraphServiceDiff(
          opContext,
          urn,
          aspectSpec,
          graphPrevious,
          graphAspect,
          event.getMetadataChangeLog(),
          leaveUnknownDestinations);
    } else {
      updateGraphService(
          opContext,
          urn,
          aspectSpec,
          graphAspect,
          event.getMetadataChangeLog(),
          leaveUnknownDestinations);
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
   * @param previousAspect the deleted value, whose edges are removed
   */
  private void handleDeleteChangeEvent(
      @Nonnull final OperationContext opContext,
      @Nonnull final MCLItem event,
      @Nullable final RecordTemplate previousAspect) {

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
        previousAspect != null ? previousAspect : event.getRecordTemplate();
    Boolean isDeletingKey = event.getAspectName().equals(entitySpec.getKeyAspectName());

    if (!aspectSpec.isTimeseries()) {
      deleteGraphData(
          opContext, urn, aspectSpec, aspect, isDeletingKey, event.getMetadataChangeLog());
    }
  }

  /**
   * Process snapshot and update graph index
   *
   * @param leaveUnknownDestinations when true, edges to entity types this version doesn't know are
   *     neither removed nor added
   */
  private void updateGraphService(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final RecordTemplate aspect,
      @Nonnull final MetadataChangeLog event,
      final boolean leaveUnknownDestinations) {
    final EntityRegistry registry = opContext.getEntityRegistry();
    Pair<List<Edge>, HashMap<Urn, Set<String>>> edgeAndRelationTypes =
        GraphIndexUtils.getEdgesAndRelationshipTypesFromAspect(
            urn,
            aspectSpec,
            aspect,
            event,
            true,
            fineGrainedLineageNotAllowedForPlatforms,
            registry);

    final List<Edge> edgesToAdd =
        knownDestinations(edgeAndRelationTypes.getFirst(), leaveUnknownDestinations, registry);
    final HashMap<Urn, Set<String>> urnToRelationshipTypesBeingAdded =
        edgeAndRelationTypes.getSecond();
    final Filter destinationFilter =
        leaveUnknownDestinations
            ? knownDestinationTypes(registry, edgesToAdd)
            : new Filter().setOr(new ConjunctiveCriterionArray());

    log.debug("Here's the relationship types found {}", urnToRelationshipTypesBeingAdded);
    if (!urnToRelationshipTypesBeingAdded.isEmpty()) {
      for (Map.Entry<Urn, Set<String>> entry : urnToRelationshipTypesBeingAdded.entrySet()) {
        graphService.removeEdgesFromNode(
            opContext,
            entry.getKey(),
            entry.getValue(),
            newRelationshipFilter(destinationFilter, RelationshipDirection.OUTGOING));
      }
      edgesToAdd.forEach(edge -> graphService.addEdge(opContext, edge));
    }
  }

  private void updateGraphServiceDiff(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final AspectSpec aspectSpec,
      @Nullable final RecordTemplate oldAspect,
      @Nonnull final RecordTemplate newAspect,
      @Nonnull final MetadataChangeLog event,
      final boolean leaveUnknownDestinations) {
    final EntityRegistry registry = opContext.getEntityRegistry();
    EdgeDiff edgeDiff =
        GraphIndexUtils.computeAspectEdgeDiff(
            urn,
            aspectSpec,
            oldAspect,
            newAspect,
            event,
            fineGrainedLineageNotAllowedForPlatforms,
            registry);

    // Edges to add
    final List<Edge> additiveDifference =
        knownDestinations(edgeDiff.getEdgesToAdd(), leaveUnknownDestinations, registry);

    // Edges to remove
    final List<Edge> subtractiveDifference =
        knownDestinations(edgeDiff.getEdgesToRemove(), leaveUnknownDestinations, registry);

    // Edges to update
    final List<Edge> mergedEdges =
        knownDestinations(edgeDiff.getEdgesToUpdate(), leaveUnknownDestinations, registry);

    // Remove any old edges that no longer exist first
    if (!subtractiveDifference.isEmpty()) {
      log.debug("Removing edges: {}", subtractiveDifference);
      subtractiveDifference.forEach(edge -> graphService.removeEdge(opContext, edge));
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
      additiveDifference.forEach(edge -> graphService.addEdge(opContext, edge));
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
      mergedEdges.forEach(edge -> graphService.upsertEdge(opContext, edge));
      opContext
          .getMetricUtils()
          .ifPresent(
              metricUtils ->
                  metricUtils.increment(
                      this.getClass(), GRAPH_DIFF_MODE_UPDATE_METRIC, mergedEdges.size()));
    }
  }

  /**
   * The aspect as stored, read from the MCL; {@code fallback} when there is none or it's
   * unreadable.
   */
  private static RecordTemplate asStored(
      @Nullable final GenericAspect stored,
      @Nonnull final AspectSpec aspectSpec,
      @Nullable final RecordTemplate fallback) {
    if (stored == null) {
      return fallback;
    }
    try {
      return GenericRecordUtils.deserializeAspect(
          stored.getValue(), stored.getContentType(), aspectSpec);
    } catch (RuntimeException unreadable) {
      return fallback;
    }
  }

  /**
   * The edges, without those whose destination's entity type this version doesn't know when {@code
   * leaveUnknownDestinations}. Only the destination's own type is checked, as {@link
   * #knownDestinationTypes} does for a delete.
   */
  private static List<Edge> knownDestinations(
      @Nonnull final List<Edge> edges,
      final boolean leaveUnknownDestinations,
      @Nonnull final EntityRegistry registry) {
    if (!leaveUnknownDestinations) {
      return edges;
    }
    return edges.stream()
        .filter(edge -> registry.findEntitySpec(edge.getDestination().getEntityType()).isPresent())
        .toList();
  }

  /**
   * Matches edges whose destination's entity type this version knows. The match on the stored type
   * is exact while registry lookups ignore case, so each type is matched as the registry names it
   * and in lower case, and the types of {@code edges} as they are spelled in their urns. An edge to
   * a known type spelled in another case, and no longer in the aspect, isn't matched.
   */
  private static Filter knownDestinationTypes(
      @Nonnull final EntityRegistry registry, @Nonnull final List<Edge> edges) {
    final Set<String> types = new TreeSet<>();
    registry
        .getEntitySpecs()
        .values()
        .forEach(
            spec -> {
              types.add(spec.getName());
              types.add(spec.getName().toLowerCase());
            });
    edges.forEach(edge -> types.add(edge.getDestination().getEntityType()));
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(
                            CriterionUtils.buildCriterion(
                                GraphQueryConstants.DESTINATION + ".entityType",
                                Condition.EQUAL,
                                types)))));
  }

  private static boolean referencesUnknownEntityType(
      @Nullable final RecordTemplate aspect, @Nonnull final EntityRegistry registry) {
    return aspect != null && handling().aspectReferencesUnknownEntityType(aspect, registry);
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
              urn,
              aspectSpec,
              aspect,
              event,
              true,
              fineGrainedLineageNotAllowedForPlatforms,
              opContext.getEntityRegistry());

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
}
