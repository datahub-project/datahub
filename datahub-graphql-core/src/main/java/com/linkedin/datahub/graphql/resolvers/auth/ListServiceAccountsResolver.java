package com.linkedin.datahub.graphql.resolvers.auth;

import static com.datahub.authorization.AuthUtil.isAuthorized;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.ListServiceAccountsInput;
import com.linkedin.datahub.graphql.generated.ListServiceAccountsResult;
import com.linkedin.datahub.graphql.generated.ServiceAccount;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for listing service accounts. Service accounts are CorpUser entities with a SubTypes
 * aspect containing "SERVICE_ACCOUNT" in typeNames.
 */
@Slf4j
public class ListServiceAccountsResolver
    implements DataFetcher<CompletableFuture<ListServiceAccountsResult>> {

  private static final String SUB_TYPES_FIELD = "typeNames";
  private static final int DEFAULT_START = 0;
  private static final int DEFAULT_COUNT = 10;

  private final EntityClient _entityClient;

  public ListServiceAccountsResolver(final EntityClient entityClient) {
    this._entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListServiceAccountsResult> get(DataFetchingEnvironment environment)
      throws Exception {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();
          final ListServiceAccountsInput input =
              bindArgument(environment.getArgument("input"), ListServiceAccountsInput.class);
          final Integer start = input.getStart() != null ? input.getStart() : DEFAULT_START;
          final Integer count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;
          final String query = input.getQuery();

          if (!isAuthorized(
              context.getOperationContext(), PoliciesConfig.MANAGE_SERVICE_ACCOUNTS_PRIVILEGE)) {
            throw new AuthorizationException(
                "Unauthorized to list service accounts. Please contact your DataHub administrator.");
          }

          try {
            // Build filters to find only service accounts (subTypes.typeNames = SERVICE_ACCOUNT)
            final List<FacetFilterInput> filters =
                ImmutableList.of(
                    new FacetFilterInput(
                        SUB_TYPES_FIELD,
                        null,
                        ImmutableList.of(ServiceAccountUtils.SERVICE_ACCOUNT_SUB_TYPE),
                        false,
                        FilterOperator.EQUAL));

            // Search query - use the provided query or wildcard
            final String searchQuery = query != null && !query.isEmpty() ? query : "*";

            final SearchResult searchResult =
                _entityClient.search(
                    context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
                    Constants.CORP_USER_ENTITY_NAME,
                    searchQuery,
                    buildFilter(filters, Collections.emptyList()),
                    Collections
                        .emptyList(), // No sort criteria - createdAt not indexed for corpuser
                    start,
                    count);

            // Fetch full entity details for all service accounts
            final Set<Urn> urns =
                searchResult.getEntities().stream()
                    .map(SearchEntity::getEntity)
                    .collect(Collectors.toSet());

            final Map<Urn, EntityResponse> entityResponses =
                _entityClient.batchGetV2(
                    context.getOperationContext(),
                    Constants.CORP_USER_ENTITY_NAME,
                    urns,
                    null); // Fetch all aspects

            final List<ServiceAccount> serviceAccounts =
                searchResult.getEntities().stream()
                    .map(
                        entity -> {
                          final EntityResponse response = entityResponses.get(entity.getEntity());
                          return ServiceAccountUtils.mapToServiceAccount(response);
                        })
                    .filter(sa -> sa != null)
                    .collect(Collectors.toList());

            final ListServiceAccountsResult result = new ListServiceAccountsResult();
            result.setServiceAccounts(serviceAccounts);
            result.setStart(searchResult.getFrom());
            result.setCount(searchResult.getPageSize());
            result.setTotal(searchResult.getNumEntities());

            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list service accounts", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
