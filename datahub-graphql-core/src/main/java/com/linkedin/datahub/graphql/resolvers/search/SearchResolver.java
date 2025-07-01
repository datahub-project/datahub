package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.utils.SearchUtils.applyDefaultSearchFlags;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.SearchInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.common.mappers.SearchFlagsInputMapper;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.GroupingCriterion;
import com.linkedin.metadata.query.GroupingCriterionArray;
import com.linkedin.metadata.query.GroupingSpec;
import com.linkedin.metadata.query.SearchFlags;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Resolver responsible for resolving the 'search' field of the Query type */
@Slf4j
@RequiredArgsConstructor
public class SearchResolver implements DataFetcher<CompletableFuture<SearchResults>> {
  private static final SearchFlags SEARCH_RESOLVER_DEFAULTS =
      new SearchFlags()
          .setFulltext(true)
          .setMaxAggValues(20)
          .setSkipCache(false)
          .setSkipAggregates(false)
          .setSkipHighlighting(false)
          .setGroupingSpec(
              new GroupingSpec()
                  .setGroupingCriteria(
                      new GroupingCriterionArray(
                          new GroupingCriterion()
                              .setBaseEntityType(SCHEMA_FIELD_ENTITY_NAME)
                              .setGroupingEntityType(DATASET_ENTITY_NAME))));
  private static final int DEFAULT_START = 0;
  private static final int DEFAULT_COUNT = 10;

  private final EntityClient _entityClient;

  @Override
  @WithSpan
  public CompletableFuture<SearchResults> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final SearchInput input = bindArgument(environment.getArgument("input"), SearchInput.class);
    final String entityName = EntityTypeMapper.getName(input.getType());
    // escape forward slash since it is a reserved character in Elasticsearch
    final String sanitizedQuery = ResolverUtils.escapeForwardSlash(input.getQuery());

    final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
    final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;
    final SearchFlags searchFlags;
    com.linkedin.datahub.graphql.generated.SearchFlags inputFlags = input.getSearchFlags();
    if (inputFlags != null) {
      searchFlags = SearchFlagsInputMapper.INSTANCE.apply(context, inputFlags);
    } else {
      searchFlags = applyDefaultSearchFlags(null, sanitizedQuery, SEARCH_RESOLVER_DEFAULTS);
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            log.debug(
                "Executing search. entity type {}, query {}, filters: {}, orFilters: {}, start: {}, count: {}, searchFlags: {}",
                input.getType(),
                input.getQuery(),
                input.getFilters(),
                input.getOrFilters(),
                start,
                count,
                searchFlags);

            return UrnSearchResultsMapper.map(
                context,
                _entityClient.search(
                    context.getOperationContext().withSearchFlags(flags -> searchFlags),
                    entityName,
                    sanitizedQuery,
                    ResolverUtils.buildFilter(input.getFilters(), input.getOrFilters()),
                    Collections.emptyList(),
                    start,
                    count));
          } catch (Exception e) {
            log.error(
                "Failed to execute search: entity type {}, query {}, filters: {}, orFilters: {}, start: {}, count: {}, searchFlags: {}",
                input.getType(),
                input.getQuery(),
                input.getFilters(),
                input.getOrFilters(),
                start,
                count,
                searchFlags);
            throw new RuntimeException(
                "Failed to execute search: "
                    + String.format(
                        "entity type %s, query %s, filters: %s, orFilters: %s, start: %s, count: %s, searchFlags: %s",
                        input.getType(),
                        input.getQuery(),
                        input.getFilters(),
                        input.getOrFilters(),
                        start,
                        count,
                        searchFlags),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
