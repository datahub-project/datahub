package com.linkedin.metadata.graph.cache.client;

import static com.linkedin.metadata.Constants.CORP_GROUP_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATAHUB_ROLE_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.graph.cache.MembershipNeighborResult;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.utils.CriterionUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Live graph scroll fallbacks when the membership graph cache cannot satisfy a read. */
@Slf4j
public final class MembershipGraphScrollFallback {

  private MembershipGraphScrollFallback() {}

  @Nonnull
  public static MembershipNeighborResult listRelated(
      @Nonnull OperationContext opContext,
      @Nonnull MembershipReadSpec spec,
      @Nonnull String seedUrn,
      @Nonnull TraversalDirection direction,
      @Nonnull Set<String> relationshipTypes,
      int start,
      int count) {
    GraphRetriever graphRetriever = opContext.getRetrieverContext().getGraphRetriever();
    if (graphRetriever == GraphRetriever.EMPTY || relationshipTypes.isEmpty() || count <= 0) {
      return MembershipNeighborResult.fromNeighbors(List.of(), 0);
    }

    try {
      ScrollConfig scrollConfig = scrollConfigFor(seedUrn, direction, spec);
      if (scrollConfig == null) {
        return MembershipNeighborResult.fromNeighbors(List.of(), 0);
      }

      Filter anchorFilter =
          new Filter()
              .setOr(
                  new ConjunctiveCriterionArray(
                      new ConjunctiveCriterion()
                          .setAnd(
                              new CriterionArray(
                                  List.of(
                                      CriterionUtils.buildCriterion(
                                          "urn", Condition.EQUAL, seedUrn))))));

      List<MembershipNeighborResult.Neighbor> neighbors = new ArrayList<>();
      RelatedEntitiesScrollResult result = null;
      int scrollPages = 0;
      long scrollStartNanos = System.nanoTime();
      while (result == null || result.getScrollId() != null) {
        scrollPages++;
        result =
            graphRetriever.scrollRelatedEntities(
                scrollConfig.sourceEntityTypes(),
                direction == TraversalDirection.FORWARD ? anchorFilter : null,
                scrollConfig.destinationEntityTypes(),
                direction == TraversalDirection.REVERSE ? anchorFilter : null,
                relationshipTypes,
                new RelationshipFilter().setDirection(RelationshipDirection.OUTGOING),
                Edge.EDGE_SORT_CRITERION,
                result == null ? null : result.getScrollId(),
                GraphRetriever.DEFAULT_EDGE_FETCH_LIMIT,
                null,
                null);
        if (result.getEntities() != null) {
          for (var related : result.getEntities()) {
            String neighborUrn =
                direction == TraversalDirection.FORWARD
                    ? related.getDestinationUrn()
                    : related.getSourceUrn();
            neighbors.add(
                new MembershipNeighborResult.Neighbor(neighborUrn, related.getRelationshipType()));
          }
        }
      }
      recordScrollMetrics(opContext, spec, scrollPages, System.nanoTime() - scrollStartNanos);

      int total = neighbors.size();
      int resolvedStart = Math.max(start, 0);
      if (resolvedStart >= total) {
        return MembershipNeighborResult.fromNeighbors(List.of(), total);
      }
      int end = Math.min(resolvedStart + count, total);
      return MembershipNeighborResult.fromNeighbors(neighbors.subList(resolvedStart, end), total);
    } catch (Exception e) {
      log.error(
          "Failed to scroll membership neighbors for {} on graph {}",
          seedUrn,
          spec.getBinding().getGraphId(),
          e);
      return MembershipNeighborResult.miss(ReadMissReason.ABSENT);
    }
  }

  private static void recordScrollMetrics(
      @Nonnull OperationContext opContext,
      @Nonnull MembershipReadSpec spec,
      int scrollPages,
      long durationNanos) {
    if (scrollPages <= 0) {
      return;
    }
    String graphId = spec.getBinding().getGraphId();
    opContext
        .getMetricUtils()
        .ifPresent(
            metrics -> {
              metrics.incrementMicrometer(
                  "entity.graph.cache.membership_scroll.pages", scrollPages, "graphId", graphId);
              metrics.recordTimer(
                  "entity.graph.cache.membership_scroll.duration",
                  durationNanos,
                  "graphId",
                  graphId);
            });
  }

  @Nonnull
  public static Set<Urn> neighborUrns(
      @Nonnull OperationContext opContext,
      @Nonnull MembershipReadSpec spec,
      @Nonnull String seedUrn,
      @Nonnull TraversalDirection direction,
      @Nonnull Set<String> relationshipTypes) {
    MembershipNeighborResult result =
        listRelated(opContext, spec, seedUrn, direction, relationshipTypes, 0, Integer.MAX_VALUE);
    LinkedHashSet<Urn> urns = new LinkedHashSet<>();
    for (MembershipNeighborResult.Neighbor neighbor : result.neighborsOrEmpty()) {
      urns.add(UrnUtils.getUrn(neighbor.neighborUrn()));
    }
    return urns;
  }

  private static ScrollConfig scrollConfigFor(
      @Nonnull String seedUrn,
      @Nonnull TraversalDirection direction,
      @Nonnull MembershipReadSpec spec) {
    String entityType = UrnUtils.getUrn(seedUrn).getEntityType();
    return switch (entityType) {
      case CORP_USER_ENTITY_NAME -> new ScrollConfig(
          spec.getScrollUserEntityTypes(), spec.getScrollGroupEntityTypes());
      case CORP_GROUP_ENTITY_NAME -> direction == TraversalDirection.FORWARD
          ? new ScrollConfig(spec.getScrollGroupEntityTypes(), spec.getScrollRoleEntityTypes())
          : new ScrollConfig(spec.getScrollUserEntityTypes(), spec.getScrollGroupEntityTypes());
      case DATAHUB_ROLE_ENTITY_NAME -> new ScrollConfig(
          spec.getScrollUserEntityTypes(), spec.getScrollRoleEntityTypes());
      default -> null;
    };
  }

  private record ScrollConfig(
      @Nonnull Set<String> sourceEntityTypes, @Nonnull Set<String> destinationEntityTypes) {}
}
