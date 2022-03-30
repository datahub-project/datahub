package com.linkedin.metadata.graph;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.search.utils.QueryUtils;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;

import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;


public interface GraphService {
  /**
   * Return lineage registry to construct graph index
   */
  LineageRegistry getLineageRegistry();

  /**
   * Adds an edge to the graph. This creates the source and destination nodes, if they do not exist.
   */
  void addEdge(final Edge edge);

  /**
   * Find related entities (nodes) connected to a source entity via edges of given relationship types. Related entities
   * can be filtered by source and destination type (use `null` for any type), by source and destination entity filter
   * and relationship filter. Pagination of the result is controlled via `offset` and `count`.
   *
   * Starting from a node as the source entity, determined by `sourceType` and `sourceEntityFilter`,
   * related entities are found along the direction of edges (`RelationshipDirection.OUTGOING`) or in opposite
   * direction of edges (`RelationshipDirection.INCOMING`). The destination entities are further filtered by `destinationType`
   * and `destinationEntityFilter`, and then returned as related entities.
   *
   * This does not return duplicate related entities, even if entities are connected to source entities via multiple edges.
   * An empty list of relationship types returns an empty result.
   *
   * In other words, the source and destination entity is not to be understood as the source and destination of the edge,
   * but as the source and destination of "finding related entities", where always the destination entities are returned.
   * This understanding is important when it comes to `RelationshipDirection.INCOMING`. The origin of the edge becomes
   * the destination entity and the source entity is where the edge points to.
   *
   * Example I:
   *   dataset one --DownstreamOf-> dataset two --DownstreamOf-> dataset three
   *
   *   findRelatedEntities(null, EMPTY_FILTER, null, EMPTY_FILTER, ["DownstreamOf"], RelationshipFilter.setDirection(RelationshipDirection.OUTGOING), 0, 100)
   *   - RelatedEntity("DownstreamOf", "dataset two")
   *   - RelatedEntity("DownstreamOf", "dataset three")
   *
   *   findRelatedEntities(null, EMPTY_FILTER, null, EMPTY_FILTER, ["DownstreamOf"], RelationshipFilter.setDirection(RelationshipDirection.INCOMING), 0, 100)
   *   - RelatedEntity("DownstreamOf", "dataset one")
   *   - RelatedEntity("DownstreamOf", "dataset two")
   *
   * Example II:
   *   dataset one --HasOwner-> user one
   *
   *   findRelatedEntities(null, EMPTY_FILTER, null, EMPTY_FILTER, ["HasOwner"], RelationshipFilter.setDirection(RelationshipDirection.OUTGOING), 0, 100)
   *   - RelatedEntity("HasOwner", "user one")
   *
   *   findRelatedEntities(null, EMPTY_FILTER, null, EMPTY_FILTER, ["HasOwner"], RelationshipFilter.setDirection(RelationshipDirection.INCOMING), 0, 100)
   *   - RelatedEntity("HasOwner", "dataset one")
   *
   * Calling this method with {@link com.linkedin.metadata.query.RelationshipDirection} `UNDIRECTED` in `relationshipFilter`
   * is equivalent to the union of `OUTGOING` and `INCOMING` (without duplicates).
   *
   * Example III:
   *   findRelatedEntities(null, EMPTY_FILTER, null, EMPTY_FILTER, ["DownstreamOf"], RelationshipFilter.setDirection(RelationshipDirection.UNDIRECTED), 0, 100)
   *   - RelatedEntity("DownstreamOf", "dataset one")
   *   - RelatedEntity("DownstreamOf", "dataset two")
   *   - RelatedEntity("DownstreamOf", "dataset three")
   */
  @Nonnull
  RelatedEntitiesResult findRelatedEntities(@Nullable final String sourceType, @Nonnull final Filter sourceEntityFilter,
      @Nullable final String destinationType, @Nonnull final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes, @Nonnull final RelationshipFilter relationshipFilter,
      final int offset, final int count);

  /**
   * Traverse from the entityUrn towards the input direction up to maxHops number of hops
   * Abstracts away the concept of relationship types
   *
   * Unless overridden, it uses the lineage registry to fetch valid edge types and queries for them
   */
  @Nonnull
  default EntityLineageResult getLineage(@Nonnull Urn entityUrn, @Nonnull LineageDirection direction, int offset,
      int count, int maxHops) {
    if (maxHops > 1) {
      throw new UnsupportedOperationException(
          String.format("More than 1 hop is not supported for %s", this.getClass().getSimpleName()));
    }
    List<LineageRegistry.EdgeInfo> edgesToFetch =
        getLineageRegistry().getLineageRelationships(entityUrn.getEntityType(), direction);
    Map<Boolean, List<LineageRegistry.EdgeInfo>> edgesByDirection = edgesToFetch.stream()
        .collect(Collectors.partitioningBy(edgeInfo -> edgeInfo.getDirection() == RelationshipDirection.OUTGOING));
    EntityLineageResult result = new EntityLineageResult().setStart(offset)
        .setCount(count)
        .setRelationships(new LineageRelationshipArray())
        .setTotal(0);
    Set<String> visitedUrns = new HashSet<>();

    // Outgoing edges
    if (!CollectionUtils.isEmpty(edgesByDirection.get(true))) {
      List<String> relationshipTypes =
          edgesByDirection.get(true).stream().map(LineageRegistry.EdgeInfo::getType).collect(Collectors.toList());
      // Fetch outgoing edges
      RelatedEntitiesResult outgoingEdges =
          findRelatedEntities(null, newFilter("urn", entityUrn.toString()), null, QueryUtils.EMPTY_FILTER,
              relationshipTypes, newRelationshipFilter(QueryUtils.EMPTY_FILTER, RelationshipDirection.OUTGOING), offset,
              count);

      // Update offset and count to fetch the correct number of incoming edges below
      offset = Math.max(0, offset - outgoingEdges.getTotal());
      count = Math.max(0, count - outgoingEdges.getEntities().size());

      result.setTotal(result.getTotal() + outgoingEdges.getTotal());
      outgoingEdges.getEntities().forEach(entity -> {
        visitedUrns.add(entity.getUrn());
        try {
          result.getRelationships()
              .add(new LineageRelationship().setEntity(Urn.createFromString(entity.getUrn()))
                  .setType(entity.getRelationshipType()));
        } catch (URISyntaxException ignored) {
        }
      });
    }

    // Incoming edges
    if (!CollectionUtils.isEmpty(edgesByDirection.get(false))) {
      List<String> relationshipTypes =
          edgesByDirection.get(false).stream().map(LineageRegistry.EdgeInfo::getType).collect(Collectors.toList());
      RelatedEntitiesResult incomingEdges =
          findRelatedEntities(null, newFilter("urn", entityUrn.toString()), null, QueryUtils.EMPTY_FILTER,
              relationshipTypes, newRelationshipFilter(QueryUtils.EMPTY_FILTER, RelationshipDirection.INCOMING), offset,
              count);
      result.setTotal(result.getTotal() + incomingEdges.getTotal());
      incomingEdges.getEntities().forEach(entity -> {
        if (visitedUrns.contains(entity.getUrn())) {
          return;
        }
        visitedUrns.add(entity.getUrn());
        try {
          result.getRelationships()
              .add(new LineageRelationship().setEntity(Urn.createFromString(entity.getUrn()))
                  .setType(entity.getRelationshipType()));
        } catch (URISyntaxException ignored) {
        }
      });
    }

    return result;
  }

  /**
   * Removes the given node (if it exists) as well as all edges (incoming and outgoing) of the node.
   */
  void removeNode(@Nonnull final Urn urn);

  /**
   * Removes edges of the given relationship types from the given node after applying the relationship filter.
   *
   * An empty list of relationship types removes nothing from the node.
   *
   * Calling this method with a {@link com.linkedin.metadata.query.RelationshipDirection} `UNDIRECTED` in `relationshipFilter`
   * is equivalent to the union of `OUTGOING` and `INCOMING` (without duplicates).
   */
  void removeEdgesFromNode(@Nonnull final Urn urn, @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter);

  void configure();

  /**
   * Removes all edges and nodes from the graph.
   */
  void clear();

  /**
   * Whether or not this graph service supports multi-hop
   */
  default boolean supportsMultiHop() {
    return false;
  }
}
