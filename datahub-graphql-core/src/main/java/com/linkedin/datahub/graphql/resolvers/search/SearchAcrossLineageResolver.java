package com.linkedin.datahub.graphql.resolvers.search;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.LineageDirection;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageInput;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageResults;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchAcrossLineageResultsMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.SEARCHABLE_ENTITY_TYPES;


/**
 * Resolver responsible for resolving 'searchAcrossEntities' field of the Query type
 */
@Slf4j
@RequiredArgsConstructor
public class SearchAcrossLineageResolver
    implements DataFetcher<CompletableFuture<SearchAcrossLineageResults>> {

  private static final int DEFAULT_START = 0;
  private static final int DEFAULT_COUNT = 10;

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<SearchAcrossLineageResults> get(DataFetchingEnvironment environment)
      throws URISyntaxException {
    final SearchAcrossLineageInput input =
        bindArgument(environment.getArgument("input"), SearchAcrossLineageInput.class);

    final QueryContext context = environment.getContext();
    final Urn urn = Urn.createFromString(input.getUrn());

    final LineageDirection lineageDirection = input.getDirection();

    List<EntityType> entityTypes =
        (input.getTypes() == null || input.getTypes().isEmpty()) ? SEARCHABLE_ENTITY_TYPES : input.getTypes();
    List<String> entityNames = entityTypes.stream().map(EntityTypeMapper::getName).collect(Collectors.toList());

    // escape forward slash since it is a reserved character in Elasticsearch
    final String sanitizedQuery = input.getQuery() != null ? ResolverUtils.escapeForwardSlash(input.getQuery()) : null;

    final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
    final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;

    com.linkedin.metadata.graph.LineageDirection resolvedDirection =
        com.linkedin.metadata.graph.LineageDirection.valueOf(lineageDirection.toString());
    return CompletableFuture.supplyAsync(() -> {
      try {
        log.debug(
            "Executing search across relationships: source urn {}, direction {}, entity types {}, query {}, filters: {}, start: {}, count: {}",
            urn, resolvedDirection, input.getTypes(), input.getQuery(), input.getFilters(), start, count);
        return UrnSearchAcrossLineageResultsMapper.map(
            _entityClient.searchAcrossLineage(urn, resolvedDirection, entityNames, sanitizedQuery,
                ResolverUtils.buildFilter(input.getFilters()), null, start, count,
                ResolverUtils.getAuthentication(environment)));
      } catch (RemoteInvocationException e) {
        log.error(
            "Failed to execute search across relationships: source urn {}, direction {}, entity types {}, query {}, filters: {}, start: {}, count: {}",
            urn, resolvedDirection, input.getTypes(), input.getQuery(), input.getFilters(), start, count);
        throw new RuntimeException("Failed to execute search across relationships: " + String.format(
            "source urn %s, direction %s, entity types %s, query %s, filters: %s, start: %s, count: %s", urn,
            resolvedDirection, input.getTypes(), input.getQuery(), input.getFilters(), start, count), e);
      }
    });
  }
}
