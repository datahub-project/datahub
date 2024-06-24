package com.linkedin.datahub.graphql.resolvers.user;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.ListUsersInput;
import com.linkedin.datahub.graphql.generated.ListUsersResult;
import com.linkedin.datahub.graphql.types.corpuser.mappers.CorpUserMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

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
                      Collections.emptyMap(),
                      start,
                      count);

              // Then, get hydrate all users.
              final Map<Urn, EntityResponse> entities =
                  _entityClient.batchGetV2(
                      context.getOperationContext(),
                      CORP_USER_ENTITY_NAME,
                      new HashSet<>(
                          gmsResult.getEntities().stream()
                              .map(SearchEntity::getEntity)
                              .collect(Collectors.toList())),
                      null);

              // Now that we have entities we can bind this to a result.
              final ListUsersResult result = new ListUsersResult();
              result.setStart(gmsResult.getFrom());
              result.setCount(gmsResult.getPageSize());
              result.setTotal(gmsResult.getNumEntities());
              result.setUsers(mapEntities(context, entities.values()));
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

  private static List<CorpUser> mapEntities(
      @Nullable QueryContext context, final Collection<EntityResponse> entities) {
    return entities.stream().map(e -> CorpUserMapper.map(context, e)).collect(Collectors.toList());
  }
}
