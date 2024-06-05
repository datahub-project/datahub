package com.linkedin.datahub.graphql.resolvers.search;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ScrollAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.ViewService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

/** Resolver responsible for resolving 'searchAcrossEntities' field of the Query type */
@Slf4j
@RequiredArgsConstructor
public class ScrollAcrossEntitiesResolver implements DataFetcher<CompletableFuture<ScrollResults>> {

  private final EntityClient _entityClient;
  private final ViewService _viewService;

  @Override
  public CompletableFuture<ScrollResults> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final ScrollAcrossEntitiesInput input =
        bindArgument(environment.getArgument("input"), ScrollAcrossEntitiesInput.class);
    return SearchUtils.scrollAcrossEntities(
        context,
        _entityClient,
        _viewService,
        input.getTypes(),
        input.getQuery(),
        ResolverUtils.buildFilter(null, input.getOrFilters()),
        input.getViewUrn(),
        input.getSearchFlags(),
        input.getCount(),
        input.getScrollId(),
        input.getKeepAlive(),
        this.getClass().getSimpleName());
  }
}
