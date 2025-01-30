package com.linkedin.datahub.graphql.resolvers.siblings;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
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
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Resolver that executes a searchAcrossEntities only on an entity's siblings */
@Slf4j
@RequiredArgsConstructor
public class SiblingsSearchResolver implements DataFetcher<CompletableFuture<ScrollResults>> {

  private static final String SIBLINGS_FIELD_NAME = "siblings";

  private final EntityClient _entityClient;
  private final ViewService _viewService;

  @Override
  public CompletableFuture<ScrollResults> get(DataFetchingEnvironment environment) {
    final Entity entity = environment.getSource();
    final QueryContext context = environment.getContext();
    final ScrollAcrossEntitiesInput input =
        bindArgument(environment.getArgument("input"), ScrollAcrossEntitiesInput.class);

    final Criterion siblingsFilter =
        CriterionUtils.buildCriterion(SIBLINGS_FIELD_NAME, Condition.EQUAL, entity.getUrn());
    final Filter baseFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(siblingsFilter))));
    final Filter inputFilter = ResolverUtils.buildFilter(null, input.getOrFilters());

    return SearchUtils.scrollAcrossEntities(
        context,
        _entityClient,
        _viewService,
        input.getTypes(),
        input.getQuery(),
        SearchUtils.combineFilters(inputFilter, baseFilter),
        input.getViewUrn(),
        input.getSearchFlags(),
        input.getCount(),
        input.getScrollId(),
        input.getKeepAlive(),
        this.getClass().getSimpleName());
  }
}
