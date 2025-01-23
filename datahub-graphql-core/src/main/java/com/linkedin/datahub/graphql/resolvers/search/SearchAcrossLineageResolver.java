package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.*;
import static com.linkedin.metadata.Constants.QUERY_ENTITY_NAME;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.LineageDirection;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageInput;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.common.mappers.LineageFlagsInputMapper;
import com.linkedin.datahub.graphql.types.common.mappers.SearchFlagsInputMapper;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchAcrossLineageResultsMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.VisibleForTesting;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Resolver responsible for resolving 'searchAcrossEntities' field of the Query type */
@Slf4j
public class SearchAcrossLineageResolver
    implements DataFetcher<CompletableFuture<SearchAcrossLineageResults>> {

  private static final int DEFAULT_START = 0;
  private static final int DEFAULT_COUNT = 10;

  private static final Set<String> TRANSIENT_ENTITIES = ImmutableSet.of(QUERY_ENTITY_NAME);

  private final EntityClient _entityClient;

  private final EntityRegistry _entityRegistry;

  @VisibleForTesting final Set<String> _allEntities;
  private final List<String> _allowedEntities;

  public SearchAcrossLineageResolver(EntityClient entityClient, EntityRegistry entityRegistry) {
    this._entityClient = entityClient;
    this._entityRegistry = entityRegistry;
    this._allEntities =
        entityRegistry.getEntitySpecs().values().stream()
            .map(EntitySpec::getName)
            .collect(Collectors.toSet());

    this._allowedEntities =
        this._allEntities.stream()
            .filter(e -> !TRANSIENT_ENTITIES.contains(e))
            .collect(Collectors.toList());
  }

  private List<String> getEntityNamesFromInput(List<EntityType> inputTypes) {
    if (inputTypes != null && !inputTypes.isEmpty()) {
      return inputTypes.stream().map(EntityTypeMapper::getName).collect(Collectors.toList());
    } else {
      return this._allowedEntities;
    }
  }

  @Override
  public CompletableFuture<SearchAcrossLineageResults> get(DataFetchingEnvironment environment)
      throws URISyntaxException {
    log.debug("Entering search across lineage graphql resolver");
    final QueryContext context = environment.getContext();

    final SearchAcrossLineageInput input =
        bindArgument(environment.getArgument("input"), SearchAcrossLineageInput.class);

    final Urn urn = Urn.createFromString(input.getUrn());

    final LineageDirection lineageDirection = input.getDirection();

    List<String> entityNames = getEntityNamesFromInput(input.getTypes());

    // escape forward slash since it is a reserved character in Elasticsearch
    final String sanitizedQuery =
        input.getQuery() != null ? ResolverUtils.escapeForwardSlash(input.getQuery()) : null;

    final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
    final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;
    final List<AndFilterInput> filters =
        input.getOrFilters() != null ? input.getOrFilters() : new ArrayList<>();
    final List<FacetFilterInput> facetFilters =
        filters.stream()
            .map(AndFilterInput::getAnd)
            .flatMap(List::stream)
            .collect(Collectors.toList());
    final Integer maxHops = getMaxHops(facetFilters);

    @Nullable
    Long startTimeMillis = input.getStartTimeMillis() == null ? null : input.getStartTimeMillis();
    @Nullable
    Long endTimeMillis =
        ResolverUtils.getLineageEndTimeMillis(input.getStartTimeMillis(), input.getEndTimeMillis());

    final LineageFlags lineageFlags = LineageFlagsInputMapper.map(context, input.getLineageFlags());
    if (lineageFlags.getStartTimeMillis() == null && startTimeMillis != null) {
      lineageFlags.setStartTimeMillis(startTimeMillis);
    }

    if (lineageFlags.getEndTimeMillis() == null && endTimeMillis != null) {
      lineageFlags.setEndTimeMillis(endTimeMillis);
    }

    com.linkedin.metadata.graph.LineageDirection resolvedDirection =
        com.linkedin.metadata.graph.LineageDirection.valueOf(lineageDirection.toString());
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            log.debug(
                "Executing search across relationships: source urn {}, direction {}, entity types {}, query {}, filters: {}, start: {}, count: {}",
                urn,
                resolvedDirection,
                input.getTypes(),
                input.getQuery(),
                filters,
                start,
                count);

            final Filter filter =
                ResolverUtils.buildFilter(input.getFilters(), input.getOrFilters());
            final SearchFlags searchFlags;
            com.linkedin.datahub.graphql.generated.SearchFlags inputFlags = input.getSearchFlags();
            if (inputFlags != null) {
              searchFlags = SearchFlagsInputMapper.INSTANCE.apply(context, inputFlags);
              if (inputFlags.getSkipHighlighting() == null) {
                searchFlags.setSkipHighlighting(true);
              }
            } else {
              searchFlags = new SearchFlags().setFulltext(true).setSkipHighlighting(true);
            }
            LineageSearchResult salResults =
                _entityClient.searchAcrossLineage(
                    context
                        .getOperationContext()
                        .withSearchFlags(flags -> searchFlags)
                        .withLineageFlags(flags -> lineageFlags),
                    urn,
                    resolvedDirection,
                    entityNames,
                    sanitizedQuery,
                    maxHops,
                    filter,
                    null,
                    start,
                    count);

            return UrnSearchAcrossLineageResultsMapper.map(context, salResults);
          } catch (RemoteInvocationException e) {
            log.error(
                "Failed to execute search across relationships: source urn {}, direction {}, entity types {}, query {}, filters: {}, start: {}, count: {}",
                urn,
                resolvedDirection,
                input.getTypes(),
                input.getQuery(),
                filters,
                start,
                count);
            throw new RuntimeException(
                "Failed to execute search across relationships: "
                    + String.format(
                        "source urn %s, direction %s, entity types %s, query %s, filters: %s, start: %s, count: %s",
                        urn,
                        resolvedDirection,
                        input.getTypes(),
                        input.getQuery(),
                        filters,
                        start,
                        count),
                e);
          } finally {
            log.debug("Returning from search across lineage resolver");
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
