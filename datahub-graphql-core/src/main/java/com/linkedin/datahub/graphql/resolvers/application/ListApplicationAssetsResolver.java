package com.linkedin.datahub.graphql.resolvers.application;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.search.utils.QueryUtils.buildFilterWithUrns;

import com.google.common.collect.ImmutableList;
import com.linkedin.application.ApplicationAssociation;
import com.linkedin.application.ApplicationProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Application;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.common.mappers.SearchFlagsInputMapper;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver responsible for getting the assets belonging to an Application. Get the assets from the
 * Application aspect, then use search to query and filter for specific assets.
 */
@Slf4j
@RequiredArgsConstructor
public class ListApplicationAssetsResolver
    implements DataFetcher<CompletableFuture<SearchResults>> {

  private static final int DEFAULT_START = 0;
  private static final int DEFAULT_COUNT = 10;

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<SearchResults> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    // get urn from either input or source (in the case of "entities" field)
    final String urn =
        environment.getArgument("urn") != null
            ? environment.getArgument("urn")
            : ((Application) environment.getSource()).getUrn();
    final Urn applicationUrn = UrnUtils.getUrn(urn);
    final SearchAcrossEntitiesInput input =
        bindArgument(environment.getArgument("input"), SearchAcrossEntitiesInput.class);

    // 1. Get urns of assets belonging to Application using an aspect query
    List<Urn> assetUrns = new ArrayList<>();
    try {
      final EntityResponse entityResponse =
          _entityClient.getV2(
              context.getOperationContext(),
              Constants.APPLICATION_ENTITY_NAME,
              applicationUrn,
              Collections.singleton(Constants.APPLICATION_PROPERTIES_ASPECT_NAME));
      if (entityResponse != null
          && entityResponse
              .getAspects()
              .containsKey(Constants.APPLICATION_PROPERTIES_ASPECT_NAME)) {
        final DataMap data =
            entityResponse
                .getAspects()
                .get(Constants.APPLICATION_PROPERTIES_ASPECT_NAME)
                .getValue()
                .data();
        final ApplicationProperties applicationProperties = new ApplicationProperties(data);
        if (applicationProperties.hasAssets()) {
          assetUrns.addAll(
              applicationProperties.getAssets().stream()
                  .map(ApplicationAssociation::getDestinationUrn)
                  .collect(Collectors.toList()));
        }
      }
    } catch (Exception e) {
      log.error(String.format("Failed to list application assets with urn %s", applicationUrn), e);
      throw new RuntimeException(
          String.format("Failed to list application assets with urn %s", applicationUrn), e);
    }

    // 2. Get list of entities that we should query based on filters or assets from aspect.
    List<String> entitiesToQuery =
        assetUrns.stream().map(Urn::getEntityType).distinct().collect(Collectors.toList());

    final List<EntityType> inputEntityTypes =
        (input.getTypes() == null || input.getTypes().isEmpty())
            ? ImmutableList.of()
            : input.getTypes();
    final List<String> inputEntityNames =
        inputEntityTypes.stream()
            .map(EntityTypeMapper::getName)
            .distinct()
            .collect(Collectors.toList());

    final List<String> finalEntityNames =
        inputEntityNames.size() > 0 ? inputEntityNames : entitiesToQuery;

    // escape forward slash since it is a reserved character in Elasticsearch
    final String sanitizedQuery = ResolverUtils.escapeForwardSlash(input.getQuery());

    final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
    final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // if no assets in application properties, exit early before search and return empty
          // results
          if (assetUrns.size() == 0) {
            SearchResults results = new SearchResults();
            results.setStart(start);
            results.setCount(count);
            results.setTotal(0);
            results.setSearchResults(ImmutableList.of());
            return results;
          }

          List<FacetFilterInput> filters = input.getFilters();
          final List<Urn> urnsToFilterOn = getUrnsToFilterOn(assetUrns, filters);
          // add urns from the aspect to our filters
          final Filter baseFilter = ResolverUtils.buildFilter(filters, input.getOrFilters());
          final Filter finalFilter =
              buildFilterWithUrns(
                  context.getDataHubAppConfig(), new HashSet<>(urnsToFilterOn), baseFilter);

          final SearchFlags searchFlags;
          com.linkedin.datahub.graphql.generated.SearchFlags inputFlags = input.getSearchFlags();
          if (inputFlags != null) {
            searchFlags = SearchFlagsInputMapper.INSTANCE.apply(context, inputFlags);
          } else {
            searchFlags = null;
          }

          try {
            log.debug(
                "Executing search for application assets: entity types {}, query {}, filters: {}, start: {}, count: {}",
                input.getTypes(),
                input.getQuery(),
                input.getOrFilters(),
                start,
                count);

            SearchResults results =
                UrnSearchResultsMapper.map(
                    context,
                    _entityClient.searchAcrossEntities(
                        context
                            .getOperationContext()
                            .withSearchFlags(flags -> searchFlags != null ? searchFlags : flags),
                        finalEntityNames,
                        sanitizedQuery,
                        finalFilter,
                        start,
                        count,
                        null));
            return results;
          } catch (Exception e) {
            log.error(
                "Failed to execute search for application assets: entity types {}, query {}, filters: {}, start: {}, count: {}",
                input.getTypes(),
                input.getQuery(),
                input.getOrFilters(),
                start,
                count);
            throw new RuntimeException(
                "Failed to execute search: "
                    + String.format(
                        "entity types %s, query %s, filters: %s, start: %s, count: %s",
                        input.getTypes(), input.getQuery(), input.getOrFilters(), start, count),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Check to see if our filters list has a hardcoded filter for output ports. If so, let this
   * filter determine which urns we filter search results on. Otherwise, if no output port filter is
   * found, return all asset urns as per usual.
   */
  @Nonnull
  private List<Urn> getUrnsToFilterOn(
      @Nonnull final List<Urn> assetUrns, @Nullable final List<FacetFilterInput> filters) {
    // All logic related to output ports is removed.
    // We simply return the original assetUrns list.
    return assetUrns;
  }
}
