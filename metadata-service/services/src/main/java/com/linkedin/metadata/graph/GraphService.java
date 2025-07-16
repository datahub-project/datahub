package com.linkedin.metadata.graph;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.EdgeUrnType;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.query.filter.SortCriterion;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public interface GraphService {

  GraphServiceConfiguration getGraphServiceConfig();

  /** Return lineage registry to construct graph index */
  LineageRegistry getLineageRegistry();

  /**
   * Adds an edge to the graph. This creates the source and destination nodes, if they do not exist.
   */
  void addEdge(final Edge edge);

  /**
   * Adds or updates an edge to the graph. This creates the source and destination nodes, if they do
   * not exist.
   */
  void upsertEdge(final Edge edge);

  /**
   * Remove an edge from the graph.
   *
   * @param edge the edge to delete
   */
  void removeEdge(final Edge edge);

  /**
   * Find related entities (nodes) connected to a source entity via edges of given relationship
   * types. Related entities can be filtered by source and destination type (use `null` for any
   * type), by source and destination entity filter and relationship filter. Pagination of the
   * result is controlled via `offset` and `count`.
   *
   * <p>Starting from a node as the source entity, determined by `sourceType` and
   * `sourceEntityFilter`, related entities are found along the direction of edges
   * (`RelationshipDirection.OUTGOING`) or in opposite direction of edges
   * (`RelationshipDirection.INCOMING`). The destination entities are further filtered by
   * `destinationType` and `destinationEntityFilter`, and then returned as related entities.
   *
   * <p>This does not return duplicate related entities, even if entities are connected to source
   * entities via multiple edges. An empty list of relationship types returns an empty result.
   *
   * <p>In other words, the source and destination entity is not to be understood as the source and
   * destination of the edge, but as the source and destination of "finding related entities", where
   * always the destination entities are returned. This understanding is important when it comes to
   * `RelationshipDirection.INCOMING`. The origin of the edge becomes the destination entity and the
   * source entity is where the edge points to.
   *
   * <p>Example I: dataset one --DownstreamOf-> dataset two --DownstreamOf-> dataset three
   *
   * <p>findRelatedEntities(null, EMPTY_FILTER, null, EMPTY_FILTER, ["DownstreamOf"],
   * RelationshipFilter.setDirection(RelationshipDirection.OUTGOING), 0, 100) -
   * RelatedEntity("DownstreamOf", "dataset two") - RelatedEntity("DownstreamOf", "dataset three")
   *
   * <p>findRelatedEntities(null, EMPTY_FILTER, null, EMPTY_FILTER, ["DownstreamOf"],
   * RelationshipFilter.setDirection(RelationshipDirection.INCOMING), 0, 100) -
   * RelatedEntity("DownstreamOf", "dataset one") - RelatedEntity("DownstreamOf", "dataset two")
   *
   * <p>Example II: dataset one --HasOwner-> user one
   *
   * <p>findRelatedEntities(null, EMPTY_FILTER, null, EMPTY_FILTER, ["HasOwner"],
   * RelationshipFilter.setDirection(RelationshipDirection.OUTGOING), 0, 100) -
   * RelatedEntity("HasOwner", "user one")
   *
   * <p>findRelatedEntities(null, EMPTY_FILTER, null, EMPTY_FILTER, ["HasOwner"],
   * RelationshipFilter.setDirection(RelationshipDirection.INCOMING), 0, 100) -
   * RelatedEntity("HasOwner", "dataset one")
   *
   * <p>Calling this method with {@link RelationshipDirection} `UNDIRECTED` in `relationshipFilter`
   * is equivalent to the union of `OUTGOING` and `INCOMING` (without duplicates).
   *
   * <p>Example III: findRelatedEntities(null, EMPTY_FILTER, null, EMPTY_FILTER, ["DownstreamOf"],
   * RelationshipFilter.setDirection(RelationshipDirection.UNDIRECTED), 0, 100) -
   * RelatedEntity("DownstreamOf", "dataset one") - RelatedEntity("DownstreamOf", "dataset two") -
   * RelatedEntity("DownstreamOf", "dataset three")
   */
  default @Nonnull RelatedEntitiesResult findRelatedEntities(
      @Nonnull final OperationContext opContext,
      @Nullable final Set<String> sourceTypes,
      @Nonnull final Filter sourceEntityFilter,
      @Nullable final Set<String> destinationTypes,
      @Nonnull final Filter destinationEntityFilter,
      @Nonnull final Set<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter,
      final int offset,
      @Nullable Integer count) {
    return findRelatedEntities(
        opContext,
        new GraphFilters(
            sourceEntityFilter,
            destinationEntityFilter,
            sourceTypes,
            destinationTypes,
            relationshipTypes,
            relationshipFilter),
        offset,
        count);
  }

  /**
   * Same as above with consolidated input parameter
   *
   * @param opContext
   * @param graphFilters see method above
   * @param offset
   * @param count
   * @return
   */
  @Nonnull
  RelatedEntitiesResult findRelatedEntities(
      @Nonnull final OperationContext opContext,
      @Nonnull final GraphFilters graphFilters,
      final int offset,
      @Nullable Integer count);

  /**
   * Traverse from the entityUrn towards the input direction up to maxHops number of hops Abstracts
   * away the concept of relationship types
   *
   * <p>Unless overridden, it uses the lineage registry to fetch valid edge types and queries for
   * them
   */
  @Nonnull
  default EntityLineageResult getLineage(
      @Nonnull final OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageDirection direction,
      int offset,
      @Nullable Integer count,
      int maxHops) {
    return getLineage(
        opContext,
        entityUrn,
        LineageGraphFilters.forEntityType(
            getLineageRegistry(), entityUrn.getEntityType(), direction),
        offset,
        count,
        maxHops);
  }

  @Nonnull
  EntityLineageResult getLineage(
      @Nonnull final OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageGraphFilters graphFilters,
      int offset,
      @Nullable Integer count,
      int maxHops);

  /**
   * Removes the given node (if it exists) as well as all edges (incoming and outgoing) of the node.
   */
  void removeNode(@Nonnull final OperationContext opContext, @Nonnull final Urn urn);

  /**
   * Removes edges of the given relationship types from the given node after applying the
   * relationship filter.
   *
   * <p>An empty list of relationship types removes nothing from the node.
   *
   * <p>Calling this method with a {@link RelationshipDirection} `UNDIRECTED` in
   * `relationshipFilter` is equivalent to the union of `OUTGOING` and `INCOMING` (without
   * duplicates).
   */
  void removeEdgesFromNode(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final Set<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter);

  default void configure() {}

  /** Removes all edges and nodes from the graph. */
  void clear();

  /** Whether or not this graph service supports multi-hop */
  default boolean supportsMultiHop() {
    return false;
  }

  /**
   * Set the soft-delete status for the given Urn
   *
   * @param urn URN's status to be set
   * @param removed the removed status
   * @param edgeUrnTypes which URNs to update (source, destination, lifecycleOwner, etc)
   */
  default void setEdgeStatus(
      @Nonnull Urn urn, boolean removed, @Nonnull EdgeUrnType... edgeUrnTypes) {}

  /**
   * Access graph edges
   *
   * @param sourceTypes
   * @param sourceEntityFilter
   * @param destinationTypes
   * @param destinationEntityFilter
   * @param relationshipTypes
   * @param relationshipFilter
   * @param sortCriteria
   * @param scrollId
   * @param count
   * @param startTimeMillis
   * @param endTimeMillis
   * @return
   */
  @Nonnull
  default RelatedEntitiesScrollResult scrollRelatedEntities(
      @Nonnull OperationContext opContext,
      @Nullable Set<String> sourceTypes,
      @Nonnull Filter sourceEntityFilter,
      @Nullable Set<String> destinationTypes,
      @Nonnull Filter destinationEntityFilter,
      @Nonnull Set<String> relationshipTypes,
      @Nonnull RelationshipFilter relationshipFilter,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable Integer count,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis) {
    return scrollRelatedEntities(
        opContext,
        new GraphFilters(
            sourceEntityFilter,
            destinationEntityFilter,
            sourceTypes,
            destinationTypes,
            relationshipTypes,
            relationshipFilter),
        sortCriteria,
        scrollId,
        count,
        startTimeMillis,
        endTimeMillis);
  }

  @Nonnull
  RelatedEntitiesScrollResult scrollRelatedEntities(
      @Nonnull OperationContext opContext,
      @Nonnull GraphFilters graphFilters,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable Integer count,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis);

  /**
   * Returns list of edge documents for the given graph node and relationship tuples. Non-directed
   *
   * @param opContext operation context
   * @param edgeTuples Non-directed nodes and relationship types
   * @return list of documents matching the input criteria
   */
  List<Map<String, Object>> raw(OperationContext opContext, List<EdgeTuple> edgeTuples);

  @AllArgsConstructor
  @NoArgsConstructor
  @Data
  class EdgeTuple {
    String a;
    String b;
    String relationshipType;
  }
}
