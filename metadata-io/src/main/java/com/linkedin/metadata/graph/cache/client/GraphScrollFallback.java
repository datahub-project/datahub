package com.linkedin.metadata.graph.cache.client;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.utils.CriterionUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
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
    GraphRetriever graphRetriever = opContext.getRetrieverContext().getGraphRetriever();
    if (graphRetriever == GraphRetriever.EMPTY
        || spec.getScrollSourceEntityTypes().isEmpty()
        || spec.getScrollDestinationEntityTypes().isEmpty()) {
      return new DirectChildrenResult(Set.of(), false);
    }

    try {
      Filter destinationFilter =
          new Filter()
              .setOr(
                  new ConjunctiveCriterionArray(
                      new ConjunctiveCriterion()
                          .setAnd(
                              new CriterionArray(
                                  List.of(
                                      CriterionUtils.buildCriterion(
                                          "urn", Condition.EQUAL, parentUrn.toString()))))));

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
          result.getEntities().stream()
              .map(related -> UrnUtils.getUrn(related.getSourceUrn()))
              .forEach(children::add);
        }
      }
      return new DirectChildrenResult(children, false);
    } catch (Exception e) {
      log.error(
          "Failed to scroll direct children for {} on graph {}",
          parentUrn,
          spec.getBinding().getGraphId(),
          e);
      return new DirectChildrenResult(Set.of(), true);
    }
  }

  @Nonnull
  public static Set<Urn> allDescendants(
      @Nonnull OperationContext opContext, @Nonnull HierarchyReadSpec spec, @Nonnull Urn rootUrn) {
    Set<Urn> descendants = new LinkedHashSet<>();
    collectDescendants(opContext, spec, rootUrn, descendants);
    return descendants;
  }

  private static void collectDescendants(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Urn parentUrn,
      @Nonnull Set<Urn> descendants) {
    DirectChildrenResult directChildren = directChildren(opContext, spec, parentUrn);
    for (Urn child : directChildren.getChildUrns()) {
      if (descendants.add(child)) {
        collectDescendants(opContext, spec, child, descendants);
      }
    }
  }
}
