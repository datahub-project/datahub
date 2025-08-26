package com.linkedin.datahub.graphql.resolvers.user;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ListUsersInput;
import com.linkedin.datahub.graphql.generated.ListUsersResult;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ListUsersResolver implements DataFetcher<CompletableFuture<ListUsersResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";

  private final EntityClient _entityClient;

  public ListUsersResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListUsersResult> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    if (AuthorizationUtils.canManageUsersAndGroups(context)) {
      final ListUsersInput input =
          bindArgument(environment.getArgument("input"), ListUsersInput.class);
      final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
      final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
      final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();
      List<SortCriterion> sortCriteria = SearchUtils.getSortCriteria(input.getSortInput());

      return GraphQLConcurrencyUtils.supplyAsync(
          () -> {
            try {
              // First, get all policy Urns.
              final SearchResult gmsResult =
                  _entityClient.search(
                      context
                          .getOperationContext()
                          .withSearchFlags(flags -> flags.setFulltext(true)),
                      CORP_USER_ENTITY_NAME,
                      query,
                      null,
                      sortCriteria,
                      start,
                      count);

              // Now that we have entities we can bind this to a result.
              final ListUsersResult result = new ListUsersResult();
              result.setStart(gmsResult.getFrom());
              result.setCount(gmsResult.getPageSize());
              result.setTotal(gmsResult.getNumEntities());
              result.setUsers(
                  mapUnresolvedUsers(
                      gmsResult.getEntities().stream()
                          .map(SearchEntity::getEntity)
                          .collect(Collectors.toList())));
              return result;
            } catch (Exception e) {
              throw new RuntimeException("Failed to list users", e);
            }
          },
          this.getClass().getSimpleName(),
          "get");
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private List<CorpUser> mapUnresolvedUsers(final List<Urn> entityUrns) {
    final List<CorpUser> results = new ArrayList<>();
    for (final Urn urn : entityUrns) {
      final CorpUser unresolvedUser = new CorpUser();
      unresolvedUser.setUrn(urn.toString());
      unresolvedUser.setType(EntityType.CORP_USER);
      results.add(unresolvedUser);
    }
    return results;
  }
}
