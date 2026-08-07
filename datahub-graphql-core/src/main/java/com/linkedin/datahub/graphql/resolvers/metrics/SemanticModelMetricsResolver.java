package com.linkedin.datahub.graphql.resolvers.metrics;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ScrollAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.utils.CriterionUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class SemanticModelMetricsResolver implements DataFetcher<CompletableFuture<ScrollResults>> {

  static final String SEMANTIC_MODEL_FIELD_NAME = "semanticModel";
  static final String HAS_PARENT_METRIC_FIELD_NAME = "hasParentMetric";

  private final EntityClient _entityClient;
  private final ViewService _viewService;

  @Override
  public CompletableFuture<ScrollResults> get(DataFetchingEnvironment environment) {
    final Entity entity = environment.getSource();
    final QueryContext context = getQueryContext(environment);
    final ScrollAcrossEntitiesInput input =
        bindArgument(environment.getArgument("input"), ScrollAcrossEntitiesInput.class);

    final Criterion semanticModelFilter =
        CriterionUtils.buildCriterion(SEMANTIC_MODEL_FIELD_NAME, Condition.EQUAL, entity.getUrn());
    final Criterion noParentFilter =
        CriterionUtils.buildCriterion(HAS_PARENT_METRIC_FIELD_NAME, Condition.EQUAL, "false");
    final Filter baseFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(semanticModelFilter, noParentFilter))));
    final Filter inputFilter = ResolverUtils.buildFilter(null, input.getOrFilters());

    return SearchUtils.scrollAcrossEntities(
        context,
        _entityClient,
        _viewService,
        ImmutableList.of(EntityType.METRIC),
        input.getQuery(),
        SearchUtils.combineFilters(inputFilter, baseFilter),
        input.getViewUrn(),
        input.getSearchFlags(),
        input.getCount(),
        input.getScrollId(),
        input.getKeepAlive(),
        List.of(),
        List.of(),
        this.getClass().getSimpleName());
  }
}
