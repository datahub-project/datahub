package com.linkedin.datahub.graphql.resolvers.group;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.ListGroupsInput;
import com.linkedin.datahub.graphql.generated.ListGroupsResult;
import com.linkedin.datahub.graphql.types.corpgroup.mappers.CorpGroupSnapshotMapper;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

public class ListGroupsResolver implements DataFetcher<CompletableFuture<ListGroupsResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;

  private final RestliEntityClient _entityClient;

  public ListGroupsResolver(final RestliEntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListGroupsResult> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    if (AuthorizationUtils.canManageUsersAndGroups(context)) {
      final ListGroupsInput input = bindArgument(environment.getArgument("input"), ListGroupsInput.class);
      final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
      final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();

      return CompletableFuture.supplyAsync(() -> {
        try {
          // First, get all group Urns.
          final ListResult gmsResult =
              _entityClient.list(Constants.CORP_GROUP_ENTITY_NAME, Collections.emptyMap(), start, count, context.getActor());

          // Then, get hydrate all groups.
          final Map<Urn, Entity> entities = _entityClient.batchGet(new HashSet<>(gmsResult.getEntities()), context.getActor());

          // Now that we have entities we can bind this to a result.
          final ListGroupsResult result = new ListGroupsResult();
          result.setStart(gmsResult.getStart());
          result.setCount(gmsResult.getCount());
          result.setTotal(gmsResult.getTotal());
          result.setGroups(mapEntities(entities.values()));
          return result;
        } catch (Exception e) {
          throw new RuntimeException("Failed to list groups", e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private List<CorpGroup> mapEntities(final Collection<Entity> entities) {
    final List<CorpGroup> results = new ArrayList<>();
    for (final Entity entity : entities) {
      final CorpGroupSnapshot snapshot = entity.getValue().getCorpGroupSnapshot();
      results.add(mapCorpGroupSnapshot(snapshot));
    }
    return results;
  }

  private CorpGroup mapCorpGroupSnapshot(final CorpGroupSnapshot snapshot) {
    return CorpGroupSnapshotMapper.map(snapshot);
  }
}
