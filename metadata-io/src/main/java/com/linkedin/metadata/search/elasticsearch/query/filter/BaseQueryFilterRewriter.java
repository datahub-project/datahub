package com.linkedin.metadata.search.elasticsearch.query.filter;

import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;

@Slf4j
public abstract class BaseQueryFilterRewriter implements QueryFilterRewriter {

  protected <T extends QueryBuilder> T expandUrnsByGraph(
      @Nonnull OperationContext opContext,
      T queryBuilder,
      List<String> relationshipTypes,
      RelationshipDirection relationshipDirection,
      int pageSize,
      int limit) {

    if (matchTermsQueryFieldName(queryBuilder, getRewriterFieldNames())) {
      return (T)
          expandTerms(
              opContext,
              (TermsQueryBuilder) queryBuilder,
              relationshipTypes,
              relationshipDirection,
              pageSize,
              limit);
    } else if (queryBuilder instanceof BoolQueryBuilder) {
      return (T)
          handleNestedFilters(
              opContext,
              (BoolQueryBuilder) queryBuilder,
              relationshipTypes,
              relationshipDirection,
              pageSize,
              limit);
    }
    return queryBuilder;
  }

  /**
   * The assumption here is that the input query builder is part of the `filter` of a parent query
   * builder
   *
   * @param boolQueryBuilder bool query builder that is part of a filter
   * @return terms query builders needing exp
   */
  private BoolQueryBuilder handleNestedFilters(
      OperationContext opContext,
      BoolQueryBuilder boolQueryBuilder,
      List<String> relationshipTypes,
      RelationshipDirection relationshipDirection,
      int pageSize,
      int limit) {

    List<QueryBuilder> filterQueryBuilders =
        boolQueryBuilder.filter().stream()
            .map(
                qb ->
                    expandUrnsByGraph(
                        opContext, qb, relationshipTypes, relationshipDirection, pageSize, limit))
            .collect(Collectors.toList());
    List<QueryBuilder> shouldQueryBuilders =
        boolQueryBuilder.should().stream()
            .map(
                qb ->
                    expandUrnsByGraph(
                        opContext, qb, relationshipTypes, relationshipDirection, pageSize, limit))
            .collect(Collectors.toList());
    List<QueryBuilder> mustQueryBuilders =
        boolQueryBuilder.must().stream()
            .map(
                qb ->
                    expandUrnsByGraph(
                        opContext, qb, relationshipTypes, relationshipDirection, pageSize, limit))
            .collect(Collectors.toList());
    List<QueryBuilder> mustNotQueryBuilders =
        boolQueryBuilder.mustNot().stream()
            .map(
                qb ->
                    expandUrnsByGraph(
                        opContext, qb, relationshipTypes, relationshipDirection, pageSize, limit))
            .collect(Collectors.toList());

    BoolQueryBuilder expandedQueryBuilder = QueryBuilders.boolQuery();
    filterQueryBuilders.forEach(expandedQueryBuilder::filter);
    shouldQueryBuilders.forEach(expandedQueryBuilder::should);
    mustQueryBuilders.forEach(expandedQueryBuilder::must);
    mustNotQueryBuilders.forEach(expandedQueryBuilder::mustNot);
    expandedQueryBuilder.queryName(boolQueryBuilder.queryName());
    expandedQueryBuilder.adjustPureNegative(boolQueryBuilder.adjustPureNegative());
    expandedQueryBuilder.minimumShouldMatch(boolQueryBuilder.minimumShouldMatch());
    expandedQueryBuilder.boost(boolQueryBuilder.boost());

    return expandedQueryBuilder;
  }

  /**
   * Expand URNs by graph walk
   *
   * @param opContext context
   * @param termsQueryBuilder initial terms query builder
   * @param relationshipTypes relationship to walk
   * @param relationshipDirection direction to walk
   * @param pageSize pagination size
   * @param limit max results
   * @return updated query builder with expanded terms
   */
  private static QueryBuilder expandTerms(
      OperationContext opContext,
      TermsQueryBuilder termsQueryBuilder,
      List<String> relationshipTypes,
      RelationshipDirection relationshipDirection,
      int pageSize,
      int limit) {
    Set<Urn> queryUrns =
        termsQueryBuilder.values().stream()
            .map(urnObj -> UrnUtils.getUrn(urnObj.toString()))
            .collect(Collectors.toSet());
    Set<Urn> expandedUrns = new HashSet<>(queryUrns);

    if (!queryUrns.isEmpty()) {

      scrollGraph(
          opContext.getRetrieverContext().getGraphRetriever(),
          queryUrns,
          relationshipTypes,
          relationshipDirection,
          expandedUrns,
          pageSize,
          limit);

      return expandTermsQueryUrnValues(termsQueryBuilder, expandedUrns);
    }

    return termsQueryBuilder;
  }

  private static boolean matchTermsQueryFieldName(
      QueryBuilder queryBuilder, Set<String> fieldNames) {
    if (queryBuilder instanceof TermsQueryBuilder) {
      return fieldNames.stream()
          .anyMatch(fieldName -> fieldName.equals(((TermsQueryBuilder) queryBuilder).fieldName()));
    }
    return false;
  }

  private static TermsQueryBuilder expandTermsQueryUrnValues(
      TermsQueryBuilder termsQueryBuilder, Set<Urn> values) {
    return QueryBuilders.termsQuery(
            termsQueryBuilder.fieldName(), values.stream().map(Urn::toString).sorted().toArray())
        .queryName(termsQueryBuilder.queryName())
        .boost(termsQueryBuilder.boost());
  }

  private static void scrollGraph(
      @Nonnull GraphRetriever graphRetriever,
      @Nonnull Set<Urn> queryUrns,
      List<String> relationshipTypes,
      RelationshipDirection relationshipDirection,
      @Nonnull Set<Urn> visitedUrns,
      int pageSize,
      int limit) {

    List<String> entityTypes =
        queryUrns.stream().map(Urn::getEntityType).distinct().collect(Collectors.toList());
    List<String> queryUrnStrs = queryUrns.stream().map(Urn::toString).collect(Collectors.toList());

    Set<Urn> nextUrns = new HashSet<>();

    Supplier<Boolean> earlyExitCriteria =
        () -> (queryUrns.size() + visitedUrns.size() + nextUrns.size()) >= limit;

    Function<RelatedEntitiesScrollResult, Boolean> consumer =
        result -> {
          if (result != null) {
            // track next hop urns
            nextUrns.addAll(
                result.getEntities().stream()
                    .map(e -> UrnUtils.getUrn(e.asRelatedEntity().getUrn()))
                    .filter(urn -> !visitedUrns.contains(urn))
                    .collect(Collectors.toSet()));
          }

          // exit early if we have enough
          return earlyExitCriteria.get();
        };

    graphRetriever.consumeRelatedEntities(
        consumer,
        entityTypes,
        QueryUtils.newDisjunctiveFilter(buildCriterion("urn", Condition.EQUAL, queryUrnStrs)),
        entityTypes,
        EMPTY_FILTER,
        relationshipTypes,
        newRelationshipFilter(EMPTY_FILTER, relationshipDirection),
        Edge.EDGE_SORT_CRITERION,
        pageSize,
        null,
        null);

    // mark visited
    visitedUrns.addAll(queryUrns);

    if (earlyExitCriteria.get()) {
      visitedUrns.addAll(nextUrns);
    } else if (!nextUrns.isEmpty()) {
      // next hop
      scrollGraph(
          graphRetriever,
          nextUrns,
          relationshipTypes,
          relationshipDirection,
          visitedUrns,
          pageSize,
          limit);
    }
  }
}
