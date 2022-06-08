package com.linkedin.datahub.graphql.resolvers.user;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
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

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;


public class ListUsersResolver implements DataFetcher<CompletableFuture<ListUsersResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";

  private final EntityClient _entityClient;

  public ListUsersResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListUsersResult> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    if (AuthorizationUtils.canManageUsersAndGroups(context)) {
      final ListUsersInput input = bindArgument(environment.getArgument("input"), ListUsersInput.class);
      final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
      final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
      final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();

      return CompletableFuture.supplyAsync(() -> {
        try {
          // First, get all policy Urns.
          final SearchResult gmsResult =
              _entityClient.search(CORP_USER_ENTITY_NAME, query, Collections.emptyMap(), start, count, context.getAuthentication());

          // Then, get hydrate all users.
          final Map<Urn, EntityResponse> entities = _entityClient.batchGetV2(CORP_USER_ENTITY_NAME,
              new HashSet<>(gmsResult.getEntities().stream()
                  .map(SearchEntity::getEntity)
                  .collect(Collectors.toList())
              ), null, context.getAuthentication());

          // Now that we have entities we can bind this to a result.
          final ListUsersResult result = new ListUsersResult();
          result.setStart(gmsResult.getFrom());
          result.setCount(gmsResult.getPageSize());
          result.setTotal(gmsResult.getNumEntities());
          result.setUsers(mapEntities(entities.values()));
          return result;
        } catch (Exception e) {
          throw new RuntimeException("Failed to list users", e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private List<CorpUser> mapEntities(final Collection<EntityResponse> entities) {
    return entities.stream()
        .map(CorpUserMapper::map)
        .collect(Collectors.toList());
  }
}
