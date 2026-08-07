package com.linkedin.datahub.graphql.resolvers.metrics;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ScrollAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.semanticmodel.SemanticModelInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for {@code SemanticModel.metrics}: reads membership from {@code
 * semanticModelInfo.metrics}, then scrolls those metric URNs filtered to root metrics ({@code
 * hasParentMetric=false}).
 */
@Slf4j
@RequiredArgsConstructor
public class SemanticModelMetricsResolver implements DataFetcher<CompletableFuture<ScrollResults>> {

  static final String HAS_PARENT_METRIC_FIELD_NAME = "hasParentMetric";

  private final EntityClient _entityClient;
  private final ViewService _viewService;

  @Override
  public CompletableFuture<ScrollResults> get(DataFetchingEnvironment environment) {
    final Entity entity = environment.getSource();
    final QueryContext context = getQueryContext(environment);
    final ScrollAcrossEntitiesInput input =
        bindArgument(environment.getArgument("input"), ScrollAcrossEntitiesInput.class);
    final Urn semanticModelUrn = UrnUtils.getUrn(entity.getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final Set<Urn> metricUrns = loadMetricUrns(context, semanticModelUrn);
          if (metricUrns.isEmpty()) {
            final ScrollResults empty = new ScrollResults();
            empty.setCount(0);
            empty.setTotal(0);
            empty.setSearchResults(ImmutableList.of());
            return empty;
          }

          final Criterion membershipCriterion =
              CriterionUtils.buildCriterion(
                  "urn",
                  Condition.EQUAL,
                  metricUrns.stream().map(Urn::toString).collect(Collectors.toList()));
          final Criterion noParentCriterion =
              CriterionUtils.buildCriterion(HAS_PARENT_METRIC_FIELD_NAME, Condition.EQUAL, "false");
          final Filter membershipFilter =
              new Filter()
                  .setOr(
                      new ConjunctiveCriterionArray(
                          new ConjunctiveCriterion()
                              .setAnd(new CriterionArray(membershipCriterion, noParentCriterion))));
          final Filter inputFilter = ResolverUtils.buildFilter(null, input.getOrFilters());
          final Filter baseFilter = SearchUtils.combineFilters(inputFilter, membershipFilter);

          try {
            return SearchUtils.scrollAcrossEntities(
                    context,
                    _entityClient,
                    _viewService,
                    ImmutableList.of(EntityType.METRIC),
                    input.getQuery(),
                    baseFilter,
                    input.getViewUrn(),
                    input.getSearchFlags(),
                    input.getCount(),
                    input.getScrollId(),
                    input.getKeepAlive(),
                    List.of(),
                    List.of(),
                    this.getClass().getSimpleName())
                .join();
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to scroll metrics for semantic model %s", semanticModelUrn),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private Set<Urn> loadMetricUrns(final QueryContext context, final Urn semanticModelUrn) {
    try {
      final EntityResponse entityResponse =
          _entityClient.getV2(
              context.getOperationContext(),
              Constants.SEMANTIC_MODEL_ENTITY_NAME,
              semanticModelUrn,
              Collections.singleton(Constants.SEMANTIC_MODEL_INFO_ASPECT_NAME),
              false);
      if (entityResponse == null
          || !entityResponse.getAspects().containsKey(Constants.SEMANTIC_MODEL_INFO_ASPECT_NAME)) {
        return Collections.emptySet();
      }
      final DataMap data =
          entityResponse
              .getAspects()
              .get(Constants.SEMANTIC_MODEL_INFO_ASPECT_NAME)
              .getValue()
              .data();
      final SemanticModelInfo info = new SemanticModelInfo(data);
      if (!info.hasMetrics() || info.getMetrics() == null) {
        return Collections.emptySet();
      }
      return new HashSet<>(info.getMetrics());
    } catch (Exception e) {
      log.error("Failed to load semanticModelInfo.metrics for {}", semanticModelUrn, e);
      throw new RuntimeException(
          String.format("Failed to load metrics for semantic model %s", semanticModelUrn), e);
    }
  }
}
