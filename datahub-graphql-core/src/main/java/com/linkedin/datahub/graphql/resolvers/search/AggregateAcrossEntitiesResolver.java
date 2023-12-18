package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.getEntityNames;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.mapInputFlags;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.resolveView;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AggregateAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.AggregateResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
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

  @Override
  public CompletableFuture<AggregateResults> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final AggregateAcrossEntitiesInput input =
        bindArgument(environment.getArgument("input"), AggregateAcrossEntitiesInput.class);

    final List<String> entityNames = getEntityNames(input.getTypes());

    // escape forward slash since it is a reserved character in Elasticsearch
    final String sanitizedQuery = ResolverUtils.escapeForwardSlash(input.getQuery());

    return CompletableFuture.supplyAsync(
        () -> {
          final DataHubViewInfo maybeResolvedView =
              (input.getViewUrn() != null)
                  ? resolveView(
                      _viewService,
                      UrnUtils.getUrn(input.getViewUrn()),
                      context.getAuthentication())
                  : null;

          final Filter baseFilter = ResolverUtils.buildFilter(null, input.getOrFilters());

          final SearchFlags searchFlags = mapInputFlags(input.getSearchFlags());

          final List<String> facets =
              input.getFacets() != null && input.getFacets().size() > 0 ? input.getFacets() : null;

          try {
            return mapAggregateResults(
                _entityClient.searchAcrossEntities(
                    maybeResolvedView != null
                        ? SearchUtils.intersectEntityTypes(
                            entityNames, maybeResolvedView.getDefinition().getEntityTypes())
                        : entityNames,
                    sanitizedQuery,
                    maybeResolvedView != null
                        ? SearchUtils.combineFilters(
                            baseFilter, maybeResolvedView.getDefinition().getFilter())
                        : baseFilter,
                    0,
                    0, // 0 entity count because we don't want resolved entities
                    searchFlags,
                    null,
                    ResolverUtils.getAuthentication(environment),
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
        });
  }

  AggregateResults mapAggregateResults(SearchResult searchResult) {
    final AggregateResults results = new AggregateResults();
    results.setFacets(
        searchResult.getMetadata().getAggregations().stream()
            .map(MapperUtils::mapFacet)
            .collect(Collectors.toList()));

    return results;
  }
}
