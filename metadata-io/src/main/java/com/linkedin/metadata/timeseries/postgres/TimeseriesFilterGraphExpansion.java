package com.linkedin.metadata.timeseries.postgres;

import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.utils.QueryUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Expands lineage-style filter seeds via {@link GraphRetriever} — same traversal as {@link
 * com.linkedin.metadata.search.elasticsearch.query.filter.BaseQueryFilterRewriter#scrollGraph}
 * (IsPartOf + direction).
 */
public final class TimeseriesFilterGraphExpansion {

  /**
   * Mirrors {@link com.linkedin.metadata.config.search.QueryFilterRewriterConfiguration} defaults.
   */
  public static final int DEFAULT_PAGE_SIZE = 100;

  public static final int DEFAULT_LIMIT = 100;

  private TimeseriesFilterGraphExpansion() {}

  /**
   * Expands seed URNs along lineage edges to match Elasticsearch rewriter semantics for container /
   * domain-style fields.
   */
  @Nonnull
  public static Set<String> expandForLineageCondition(
      @Nonnull OperationContext opContext,
      @Nonnull Condition condition,
      @Nonnull Collection<String> seedUrnStrings) {
    GraphRetriever graphRetriever = opContext.getRetrieverContext().getGraphRetriever();
    if (graphRetriever == null || seedUrnStrings.isEmpty()) {
      return new HashSet<>(seedUrnStrings);
    }

    Set<Urn> seeds = seedUrnStrings.stream().map(UrnUtils::getUrn).collect(Collectors.toSet());
    Set<String> relationshipTypes = Set.of("IsPartOf");

    switch (condition) {
      case ANCESTORS_INCL:
        return toStrings(
            expandUrns(
                graphRetriever,
                seeds,
                relationshipTypes,
                RelationshipDirection.OUTGOING,
                DEFAULT_PAGE_SIZE,
                DEFAULT_LIMIT));
      case DESCENDANTS_INCL:
        return toStrings(
            expandUrns(
                graphRetriever,
                seeds,
                relationshipTypes,
                RelationshipDirection.INCOMING,
                DEFAULT_PAGE_SIZE,
                DEFAULT_LIMIT));
      case RELATED_INCL:
        {
          Set<Urn> a =
              expandUrns(
                  graphRetriever,
                  seeds,
                  relationshipTypes,
                  RelationshipDirection.INCOMING,
                  DEFAULT_PAGE_SIZE,
                  DEFAULT_LIMIT);
          Set<Urn> b =
              expandUrns(
                  graphRetriever,
                  seeds,
                  relationshipTypes,
                  RelationshipDirection.OUTGOING,
                  DEFAULT_PAGE_SIZE,
                  DEFAULT_LIMIT);
          a.addAll(b);
          return toStrings(a);
        }
      default:
        throw new IllegalArgumentException("Not a lineage expansion condition: " + condition);
    }
  }

  private static Set<String> toStrings(Set<Urn> urns) {
    return urns.stream().map(Urn::toString).collect(Collectors.toCollection(HashSet::new));
  }

  /**
   * Copy of {@link com.linkedin.metadata.search.elasticsearch.query.filter.BaseQueryFilterRewriter}
   * scrollGraph expansion into a flat URN set (including seeds).
   */
  @Nonnull
  private static Set<Urn> expandUrns(
      @Nonnull GraphRetriever graphRetriever,
      @Nonnull Set<Urn> queryUrns,
      @Nonnull Set<String> relationshipTypes,
      @Nonnull RelationshipDirection relationshipDirection,
      int pageSize,
      int limit) {
    Set<Urn> expandedUrns = new HashSet<>(queryUrns);
    scrollGraph(
        graphRetriever,
        queryUrns,
        relationshipTypes,
        relationshipDirection,
        expandedUrns,
        pageSize,
        limit,
        null);
    return expandedUrns;
  }

  private static void scrollGraph(
      @Nonnull GraphRetriever graphRetriever,
      @Nonnull Set<Urn> queryUrns,
      Set<String> relationshipTypes,
      RelationshipDirection relationshipDirection,
      @Nonnull Set<Urn> visitedUrns,
      int pageSize,
      int limit,
      @Nullable Object ignoredCascade) {

    Set<String> entityTypes =
        queryUrns.stream().map(Urn::getEntityType).distinct().collect(Collectors.toSet());
    List<String> queryUrnStrs = queryUrns.stream().map(Urn::toString).collect(Collectors.toList());

    Set<Urn> nextUrns = new HashSet<>();

    Supplier<Boolean> earlyExitCriteria =
        () -> (queryUrns.size() + visitedUrns.size() + nextUrns.size()) >= limit;

    Function<RelatedEntitiesScrollResult, Boolean> consumer =
        result -> {
          if (result != null) {
            nextUrns.addAll(
                result.getEntities().stream()
                    .map(e -> UrnUtils.getUrn(e.asRelatedEntity().getUrn()))
                    .filter(urn -> !visitedUrns.contains(urn))
                    .collect(Collectors.toSet()));
          }
          return earlyExitCriteria.get();
        };

    graphRetriever.consumeRelatedEntities(
        consumer,
        entityTypes,
        QueryUtils.newDisjunctiveFilter(buildCriterion("urn", Condition.EQUAL, queryUrnStrs)),
        entityTypes,
        EMPTY_FILTER,
        relationshipTypes,
        QueryUtils.newRelationshipFilter(EMPTY_FILTER, relationshipDirection),
        Edge.EDGE_SORT_CRITERION,
        pageSize,
        null,
        null);

    visitedUrns.addAll(queryUrns);

    if (earlyExitCriteria.get()) {
      visitedUrns.addAll(nextUrns);
    } else if (!nextUrns.isEmpty()) {
      scrollGraph(
          graphRetriever,
          nextUrns,
          relationshipTypes,
          relationshipDirection,
          visitedUrns,
          pageSize,
          limit,
          ignoredCascade);
    }
  }
}
