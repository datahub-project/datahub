package com.linkedin.datahub.graphql.resolvers.role;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubRole;
import com.linkedin.datahub.graphql.generated.ListRolesInput;
import com.linkedin.datahub.graphql.generated.ListRolesResult;
import com.linkedin.datahub.graphql.types.role.mappers.DataHubRoleMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;


@Slf4j
@RequiredArgsConstructor
public class ListRolesResolver implements DataFetcher<CompletableFuture<ListRolesResult>> {
  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<ListRolesResult> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    final ListRolesInput input = bindArgument(environment.getArgument("input"), ListRolesInput.class);
    final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
    final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
    final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();

    return CompletableFuture.supplyAsync(() -> {
      try {
        // First, get all role Urns.
        final SearchResult gmsResult =
            _entityClient.search(DATAHUB_ROLE_ENTITY_NAME, query, Collections.emptyMap(), start, count,
                context.getAuthentication());

        // Then, get and hydrate all users.
        final Map<Urn, EntityResponse> entities = _entityClient.batchGetV2(DATAHUB_ROLE_ENTITY_NAME,
            new HashSet<>(gmsResult.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList())),
            null, context.getAuthentication());

        final ListRolesResult result = new ListRolesResult();
        result.setStart(gmsResult.getFrom());
        result.setCount(gmsResult.getPageSize());
        result.setTotal(gmsResult.getNumEntities());
        result.setRoles(mapEntitiesToRoles(entities.values()));
        return result;
      } catch (Exception e) {
        throw new RuntimeException("Failed to list roles", e);
      }
    });
  }

  private List<DataHubRole> mapEntitiesToRoles(final Collection<EntityResponse> entities) {
    return entities.stream()
        .map(DataHubRoleMapper::map)
        .sorted(Comparator.comparing(DataHubRole::getName))
        .collect(Collectors.toList());
  }
}
