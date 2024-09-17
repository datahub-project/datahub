package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.search.utils.QueryUtils.buildFilterWithUrns;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ExtraProperty;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.common.mappers.SearchFlagsInputMapper;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductProperties;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver responsible for getting the assets belonging to a Data Product. Get the assets from the
 * Data Product aspect, then use search to query and filter for specific assets.
 */
@Slf4j
@RequiredArgsConstructor
public class ListDataProductAssetsResolver
    implements DataFetcher<CompletableFuture<SearchResults>> {

  private static final String OUTPUT_PORTS_FILTER_FIELD = "isOutputPort";
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
            : ((DataProduct) environment.getSource()).getUrn();
    final Urn dataProductUrn = UrnUtils.getUrn(urn);
    final SearchAcrossEntitiesInput input =
        bindArgument(environment.getArgument("input"), SearchAcrossEntitiesInput.class);

    // 1. Get urns of assets belonging to Data Product using an aspect query
    List<Urn> assetUrns = new ArrayList<>();
    Set<String> outputPorts = Collections.EMPTY_SET;
    try {
      final EntityResponse entityResponse =
          _entityClient.getV2(
              context.getOperationContext(),
              Constants.DATA_PRODUCT_ENTITY_NAME,
              dataProductUrn,
              Collections.singleton(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME));
      if (entityResponse != null
          && entityResponse
              .getAspects()
              .containsKey(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME)) {
        final DataMap data =
            entityResponse
                .getAspects()
                .get(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
                .getValue()
                .data();
        final DataProductProperties dataProductProperties = new DataProductProperties(data);
        if (dataProductProperties.hasAssets()) {
          assetUrns.addAll(
              dataProductProperties.getAssets().stream()
                  .map(DataProductAssociation::getDestinationUrn)
                  .collect(Collectors.toList()));
          outputPorts =
              dataProductProperties.getAssets().stream()
                  .filter(DataProductAssociation::isOutputPort)
                  .map(dpa -> dpa.getDestinationUrn().toString())
                  .collect(Collectors.toSet());
        }
      }
    } catch (Exception e) {
      log.error(String.format("Failed to list data product assets with urn %s", dataProductUrn), e);
      throw new RuntimeException(
          String.format("Failed to list data product assets with urn %s", dataProductUrn), e);
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

    Set<String> finalOutputPorts = outputPorts;
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // if no assets in data product properties, exit early before search and return empty
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
          final List<Urn> urnsToFilterOn = getUrnsToFilterOn(assetUrns, finalOutputPorts, filters);
          // need to remove output ports filter so we don't send to elastic
          if (filters != null) {
            filters.removeIf(f -> f.getField().equals(OUTPUT_PORTS_FILTER_FIELD));
          }
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
                "Executing search for data product assets: entity types {}, query {}, filters: {}, start: {}, count: {}",
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
                        null,
                        null));
            results
                .getSearchResults()
                .forEach(
                    searchResult -> {
                      if (finalOutputPorts.contains(searchResult.getEntity().getUrn())) {
                        if (searchResult.getExtraProperties() == null) {
                          searchResult.setExtraProperties(new ArrayList<>());
                        }
                        searchResult
                            .getExtraProperties()
                            .add(new ExtraProperty("isOutputPort", "true"));
                      }
                    });
            return results;
          } catch (Exception e) {
            log.error(
                "Failed to execute search for data product assets: entity types {}, query {}, filters: {}, start: {}, count: {}",
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
      @Nonnull final List<Urn> assetUrns,
      @Nonnull final Set<String> outputPortUrns,
      @Nullable final List<FacetFilterInput> filters) {
    Optional<FacetFilterInput> isOutputPort =
        filters != null
            ? filters.stream()
                .filter(f -> f.getField().equals(OUTPUT_PORTS_FILTER_FIELD))
                .findFirst()
            : Optional.empty();

    // optionally get entities that explicitly are or are not output ports
    List<Urn> urnsToFilterOn = assetUrns;
    if (isOutputPort.isPresent()) {
      if (isOutputPort.get().getValue().equals("true")) {
        urnsToFilterOn = outputPortUrns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());
      } else {
        urnsToFilterOn =
            assetUrns.stream()
                .filter(u -> !outputPortUrns.contains(u.toString()))
                .collect(Collectors.toList());
      }
    }

    return urnsToFilterOn;
  }
}
