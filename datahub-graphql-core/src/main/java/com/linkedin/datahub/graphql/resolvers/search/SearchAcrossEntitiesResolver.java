package com.linkedin.datahub.graphql.resolvers.search;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.SEARCHABLE_ENTITY_TYPES;


/**
 * Resolver responsible for resolving 'searchAcrossEntities' field of the Query type
 */
@Slf4j
@RequiredArgsConstructor
public class SearchAcrossEntitiesResolver implements DataFetcher<CompletableFuture<SearchResults>> {

  private static final int DEFAULT_START = 0;
  private static final int DEFAULT_COUNT = 10;

  private final EntityClient _entityClient;
  private final ViewService _viewService;

  @Override
  public CompletableFuture<SearchResults> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final SearchAcrossEntitiesInput input =
        bindArgument(environment.getArgument("input"), SearchAcrossEntitiesInput.class);

    final List<EntityType> entityTypes =
        (input.getTypes() == null || input.getTypes().isEmpty()) ? SEARCHABLE_ENTITY_TYPES : input.getTypes();
    final List<String> entityNames = entityTypes.stream().map(EntityTypeMapper::getName).collect(Collectors.toList());

    // escape forward slash since it is a reserved character in Elasticsearch
    final String sanitizedQuery = ResolverUtils.escapeForwardSlash(input.getQuery());

    final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
    final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;

    return CompletableFuture.supplyAsync(() -> {

      final DataHubViewInfo maybeResolvedView = (input.getViewUrn() != null)
          ? resolveView(UrnUtils.getUrn(input.getViewUrn()), context.getAuthentication())
          : null;

      final Filter baseFilter = ResolverUtils.buildFilter(input.getFilters(), input.getOrFilters());

      try {
        log.debug(
            "Executing search for multiple entities: entity types {}, query {}, filters: {}, start: {}, count: {}",
            input.getTypes(), input.getQuery(), input.getFilters(), start, count);

        return UrnSearchResultsMapper.map(_entityClient.searchAcrossEntities(
            maybeResolvedView != null
                ? SearchUtils.intersectEntityTypes(entityNames, maybeResolvedView.getDefinition().getEntityTypes())
                : entityNames,
            sanitizedQuery,
            maybeResolvedView != null
                ? SearchUtils.combineFilters(baseFilter, maybeResolvedView.getDefinition().getFilter())
                : baseFilter,
            start,
            count,
            ResolverUtils.getAuthentication(environment)));
      } catch (Exception e) {
        log.error(
            "Failed to execute search for multiple entities: entity types {}, query {}, filters: {}, start: {}, count: {}",
            input.getTypes(), input.getQuery(), input.getFilters(), start, count);
        throw new RuntimeException(
            "Failed to execute search: " + String.format("entity types %s, query %s, filters: %s, start: %s, count: %s",
                input.getTypes(), input.getQuery(), input.getFilters(), start, count), e);
      }
    });
  }

  /**
   * Attempts to resolve a View by urn. Throws {@link IllegalArgumentException} if a View with the specified
   * urn cannot be found.
   */
  private DataHubViewInfo resolveView(@Nonnull final Urn viewUrn, @Nonnull final Authentication authentication) {
    try {
      DataHubViewInfo maybeViewInfo = _viewService.getViewInfo(viewUrn, authentication);
      if (maybeViewInfo == null) {
        log.warn(String.format("Failed to resolve View with urn %s. View does not exist!", viewUrn));
      }
      return maybeViewInfo;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Caught exception while attempting to resolve View with URN %s", viewUrn), e);
    }
  }
}
