package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.getEntityNames;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.mapInputFlags;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.SearchForEntitiesByFormInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.FormUtils;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.form.FormInfo;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.FormService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Search across entities by form information. Used in order to get entities assigned to specific
 * forms as well as other details about the form and prompt states.
 */
@Slf4j
@RequiredArgsConstructor
public class SearchForEntitiesByFormResolver
    implements DataFetcher<CompletableFuture<SearchResults>> {

  private static final int DEFAULT_START = 0;
  private static final int DEFAULT_COUNT = 10;

  private final EntityClient _entityClient;
  private final FormService _formService;

  @Override
  public CompletableFuture<SearchResults> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final SearchForEntitiesByFormInput input =
        bindArgument(environment.getArgument("input"), SearchForEntitiesByFormInput.class);
    final String formUrn = input.getFormFilter().getFormUrn();

    final String query = input.getQuery() != null ? input.getQuery() : "*";
    // escape forward slash since it is a reserved character in Elasticsearch
    final String sanitizedQuery = ResolverUtils.escapeForwardSlash(query);

    final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
    final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            FormInfo formInfo =
                _formService.getFormInfo(UrnUtils.getUrn(formUrn), context.getAuthentication());
            final Filter formFilter = FormUtils.buildFormFilter(input.getFormFilter(), formInfo);
            final Filter inputFilter =
                ResolverUtils.buildFilter(ImmutableList.of(), input.getOrFilters());

            final Filter finalFilter =
                inputFilter != null
                    ? SearchUtils.combineFilters(formFilter, inputFilter)
                    : formFilter;
            SearchFlags searchFlags = mapInputFlags(input.getSearchFlags());

            return UrnSearchResultsMapper.map(
                _entityClient.searchAcrossEntities(
                    getEntityNames(null),
                    sanitizedQuery,
                    finalFilter,
                    start,
                    count,
                    searchFlags,
                    null,
                    ResolverUtils.getAuthentication(environment)));
          } catch (Exception e) {
            log.error("Failed to execute search for entities by form with urn {}", formUrn);
            throw new RuntimeException(
                String.format(
                    "Failed to execute search for entities by form with urn  %s", formUrn),
                e);
          }
        });
  }
}
