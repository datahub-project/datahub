package com.linkedin.metadata.graph.cache.client;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphEndpoints;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Live graph scroll fallbacks when the entity graph cache cannot satisfy a hierarchy read. */
@Slf4j
public final class GraphScrollFallback {

  private GraphScrollFallback() {}

  @Nonnull
  public static DirectChildrenResult directChildren(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Urn parentUrn) {
    return childrenOf(opContext, spec, Set.of(parentUrn));
  }

  /**
   * Expand all descendants under {@code rootUrn} via level-batched graph scroll.
   *
   * <p>Each frontier level issues one (paginated) scroll with an OR filter over every parent in
   * that level — O(depth) scroll rounds instead of one round-trip per node. Matches the pre-cache
   * domain BFS used for hierarchy expansion when the entity graph cache is disabled or misses.
   */
  @Nonnull
  public static Set<Urn> allDescendants(
      @Nonnull OperationContext opContext, @Nonnull HierarchyReadSpec spec, @Nonnull Urn rootUrn) {
    Set<Urn> descendants = new LinkedHashSet<>();
    Set<Urn> frontier = new LinkedHashSet<>(Set.of(rootUrn));
    while (!frontier.isEmpty()) {
      DirectChildrenResult level = childrenOf(opContext, spec, frontier);
      Set<Urn> nextFrontier = new LinkedHashSet<>();
      for (Urn child : level.getChildUrns()) {
        if (descendants.add(child)) {
          nextFrontier.add(child);
        }
      }
      frontier = nextFrontier;
    }
    return descendants;
  }

  @Nonnull
  private static DirectChildrenResult childrenOf(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Collection<Urn> parentUrns) {
    if (parentUrns.isEmpty()) {
      return new DirectChildrenResult(Set.of(), false);
    }

    GraphRetriever graphRetriever = opContext.getRetrieverContext().getGraphRetriever();
    if (graphRetriever == GraphRetriever.EMPTY
        || spec.getScrollSourceEntityTypes().isEmpty()
        || spec.getScrollDestinationEntityTypes().isEmpty()) {
      return new DirectChildrenResult(Set.of(), false);
    }

    try {
      Filter destinationFilter = urnEqualsFilter(parentUrns);
      Set<Urn> children = new LinkedHashSet<>();
      RelatedEntitiesScrollResult result = null;
      while (result == null || result.getScrollId() != null) {
        result =
            graphRetriever.scrollRelatedEntities(
                spec.getScrollSourceEntityTypes(),
                null,
                spec.getScrollDestinationEntityTypes(),
                destinationFilter,
                Set.of(spec.getRelationshipType()),
                new RelationshipFilter().setDirection(RelationshipDirection.OUTGOING),
                Edge.EDGE_SORT_CRITERION,
                result == null ? null : result.getScrollId(),
                GraphRetriever.DEFAULT_EDGE_FETCH_LIMIT,
                null,
                null);
        if (result.getEntities() != null) {
          for (var related : result.getEntities()) {
            Urn child = EntityGraphEndpoints.toUrn(related.getSourceUrn());
            if (child != null) {
              children.add(child);
            }
          }
        }
      }
      return new DirectChildrenResult(children, false);
    } catch (Exception e) {
      log.error(
          "Failed to scroll direct children for {} parent(s) on graph {}",
          parentUrns.size(),
          spec.getBinding().getGraphId(),
          e);
      return new DirectChildrenResult(Set.of(), true);
    }
  }

  /**
   * Single multi-value {@code urn} EQUAL criterion (one {@code termsQuery}), not one OR clause per
   * parent — wide frontiers must not approach ES {@code indices.query.bool.max_clause_count}.
   */
  @Nonnull
  private static Filter urnEqualsFilter(@Nonnull Collection<Urn> urns) {
    List<String> values = urns.stream().map(Urn::toString).collect(Collectors.toList());
    return QueryUtils.newDisjunctiveFilter(
        CriterionUtils.buildCriterion("urn", Condition.EQUAL, values));
  }
}
