package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.getEntityNames;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.mapInputFlags;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.resolveView;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AggregateAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.AggregateResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Executes a search query only to get a provided list of aggregations back. Does not resolve any
 * entities as results.
 */
@Slf4j
@RequiredArgsConstructor
public class AggregateAcrossEntitiesResolver
    implements DataFetcher<CompletableFuture<AggregateResults>> {

  private final EntityClient _entityClient;
  private final ViewService _viewService;
  private final FormService _formService;

  @Override
  public CompletableFuture<AggregateResults> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final AggregateAcrossEntitiesInput input =
        bindArgument(environment.getArgument("input"), AggregateAcrossEntitiesInput.class);

    final List<String> entityNames = getEntityNames(input.getTypes());

    // escape forward slash since it is a reserved character in Elasticsearch
    final String sanitizedQuery = ResolverUtils.escapeForwardSlash(input.getQuery());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final DataHubViewInfo maybeResolvedView =
              (input.getViewUrn() != null)
                  ? resolveView(
                      context.getOperationContext(),
                      _viewService,
                      UrnUtils.getUrn(input.getViewUrn()))
                  : null;

          final Filter inputFilter = ResolverUtils.buildFilter(null, input.getOrFilters());

          final SearchFlags searchFlags =
              input.getSearchFlags() != null
                  ? mapInputFlags(context, input.getSearchFlags())
                  : new SearchFlags();

          final List<String> facets =
              input.getFacets() != null && input.getFacets().size() > 0 ? input.getFacets() : null;

          // do not include default facets if we're requesting any facets specifically
          searchFlags.setIncludeDefaultFacets(facets == null || facets.size() <= 0);

          List<String> finalEntities =
              maybeResolvedView != null
                  ? SearchUtils.intersectEntityTypes(
                      entityNames, maybeResolvedView.getDefinition().getEntityTypes())
                  : entityNames;
          if (finalEntities.size() == 0) {
            return createEmptyAggregateResults();
          }

          try {
            return mapAggregateResults(
                context,
                _entityClient.searchAcrossEntities(
                    context.getOperationContext().withSearchFlags(flags -> searchFlags),
                    finalEntities,
                    sanitizedQuery,
                    maybeResolvedView != null
                        ? SearchUtils.combineFilters(
                            inputFilter, maybeResolvedView.getDefinition().getFilter())
                        : inputFilter,
                    0,
                    0, // 0 entity count because we don't want resolved entities
                    Collections.emptyList(),
                    facets));
          } catch (Exception e) {
            log.error(
                "Failed to execute aggregate across entities: entity types {}, query {}, filters: {}",
                input.getTypes(),
                input.getQuery(),
                input.getOrFilters());
            throw new RuntimeException(
                "Failed to execute aggregate across entities: "
                    + String.format(
                        "entity types %s, query %s, filters: %s",
                        input.getTypes(), input.getQuery(), input.getOrFilters()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  static AggregateResults mapAggregateResults(
      @Nullable QueryContext context, SearchResult searchResult) {
    final AggregateResults results = new AggregateResults();
    results.setFacets(
        searchResult.getMetadata().getAggregations().stream()
            .map(f -> MapperUtils.mapFacet(context, f))
            .collect(Collectors.toList()));

    return results;
  }

  AggregateResults createEmptyAggregateResults() {
    final AggregateResults result = new AggregateResults();
    result.setFacets(new ArrayList<>());
    return result;
  }
}
