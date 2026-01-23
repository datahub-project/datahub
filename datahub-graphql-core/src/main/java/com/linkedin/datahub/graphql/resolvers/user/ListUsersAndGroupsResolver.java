package com.linkedin.datahub.graphql.resolvers.user;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.utils.elasticsearch.FilterUtils;
import com.linkedin.view.DataHubViewInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for listUsersAndGroups query that searches only CORP_USER entities and enforces
 * authorization for managing users and groups.
 */
@Slf4j
@RequiredArgsConstructor
public class ListUsersAndGroupsResolver implements DataFetcher<CompletableFuture<SearchResults>> {

  private static final int DEFAULT_START = 0;
  private static final int DEFAULT_COUNT = 10;

  private final EntityClient _entityClient;
  private final ViewService _viewService;
  private final FormService _formService;

  @Override
  public CompletableFuture<SearchResults> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    if (!AuthorizationUtils.canManageUsersAndGroups(context)) {
      throw new AuthorizationException("Unauthorized to list users and groups.");
    }

    final SearchAcrossEntitiesInput input =
        bindArgument(environment.getArgument("input"), SearchAcrossEntitiesInput.class);

    // Force entity types to CORP_USER only
    final List<String> entityNames = Collections.singletonList(CORP_USER_ENTITY_NAME);

    // Default query to "*" if not provided
    final String sanitizedQuery =
        input.getQuery() != null ? ResolverUtils.escapeForwardSlash(input.getQuery()) : "*";

    final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
    final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // OSS: View resolution is supported in OSS but not commonly used
          final DataHubViewInfo maybeResolvedView =
              (input.getViewUrn() != null)
                  ? SearchUtils.resolveView(
                      context.getOperationContext(),
                      _viewService,
                      UrnUtils.getUrn(input.getViewUrn()))
                  : null;

          final Filter inputFilter =
              ResolverUtils.buildFilter(input.getFilters(), input.getOrFilters());

          // Note: Form filters are a SaaS feature in acryl-main (SearchUtils.getFormFilter)
          // In OSS, this code path is skipped since getFormFilter doesn't exist
          // When this merges to acryl-main, replace this with:
          // final Filter formFilter = SearchUtils.getFormFilter(context.getOperationContext(),
          // input.getFormFilter(), _formService);
          // final Filter baseFilter = formFilter != null ? FilterUtils.combineFilters(inputFilter,
          // formFilter) : inputFilter;
          final Filter baseFilter = inputFilter;

          SearchFlags searchFlags = SearchUtils.mapInputFlags(context, input.getSearchFlags());
          List<SortCriterion> sortCriteria = SearchUtils.getSortCriteria(input.getSortInput());

          try {
            log.debug(
                "Executing search for users: query {}, filters: {}, start: {}, count: {}",
                input.getQuery(),
                input.getOrFilters(),
                start,
                count);

            Filter finalFilter =
                maybeResolvedView != null
                    ? FilterUtils.combineFilters(
                        baseFilter, maybeResolvedView.getDefinition().getFilter())
                    : baseFilter;

            return UrnSearchResultsMapper.map(
                context,
                _entityClient.searchAcrossEntities(
                    context.getOperationContext().withSearchFlags(flags -> searchFlags),
                    entityNames,
                    sanitizedQuery,
                    finalFilter,
                    start,
                    count,
                    sortCriteria // Uses 7-param default method which works in both OSS and
                    // acryl-main
                    ));
          } catch (Exception e) {
            throw new RuntimeException("Failed to search users", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
