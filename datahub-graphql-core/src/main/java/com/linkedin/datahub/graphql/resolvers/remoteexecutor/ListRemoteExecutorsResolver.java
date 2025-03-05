package com.linkedin.datahub.graphql.resolvers.remoteexecutor;

import static com.linkedin.metadata.AcrylConstants.REMOTE_EXECUTOR_ENTITY_NAME;

import com.google.common.collect.Iterables;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ListRemoteExecutorsResult;
import com.linkedin.datahub.graphql.generated.RemoteExecutor;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.executor.RemoteExecutorStatus;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ListRemoteExecutorsResolver
    implements DataFetcher<CompletableFuture<ListRemoteExecutorsResult>> {

  private static final String REMOTE_EXECUTOR_POOL_ID_FIELD_NAME =
      Iterables.getLast(RemoteExecutorStatus.fields().executorPoolId().getPathComponents());
  private static final String SORT_BY_FIELD =
      Iterables.getLast(RemoteExecutorStatus.fields().reportedAt().getPathComponents());

  final EntityClient _entityClient;

  @Override
  public CompletableFuture<ListRemoteExecutorsResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext queryContext = environment.getContext();
    final OperationContext opContext = queryContext.getOperationContext();

    final String poolUrn =
        environment.getSource() != null ? ((Entity) environment.getSource()).getUrn() : null;
    final String poolId = poolUrn != null ? UrnUtils.getUrn(poolUrn).getId() : null;

    final int start = environment.getArgument("start");
    final int count = environment.getArgument("count");

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            final ListRemoteExecutorsResult result = new ListRemoteExecutorsResult();
            result.setStart(start);

            // 1. Get remote executors belonging to the parent pool
            final SearchResult searchResult = searchForExecutors(opContext, poolId, start, count);

            final List<Urn> executorUrns =
                searchResult.getEntities().stream().map(SearchEntity::getEntity).toList();
            result.setTotal(searchResult.getNumEntities());

            // If no results, return early
            if (executorUrns.isEmpty()) {
              result.setRemoteExecutors(Collections.emptyList());
              result.setCount(0);
              return result;
            }

            // 2. Map the executors for the retrieved urns, so gql can use the type resolver to load
            // them in
            final List<RemoteExecutor> executors =
                executorUrns.stream()
                    .map(
                        urn -> {
                          final RemoteExecutor executor = new RemoteExecutor();
                          executor.setUrn(urn.toString());
                          executor.setType(EntityType.REMOTE_EXECUTOR);
                          return executor;
                        })
                    .toList();

            // 3. Return the result
            result.setRemoteExecutors(executors);
            result.setCount(executors.size());
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list remote executors", e);
          }
        });
  }

  private SearchResult searchForExecutors(
      @Nonnull OperationContext opContext, @Nullable String poolId, int start, int count)
      throws RemoteInvocationException {
    return _entityClient.filter(
        opContext, REMOTE_EXECUTOR_ENTITY_NAME, buildFilter(poolId), buildSort(), start, count);
  }

  private Filter buildFilter(final String poolId) {
    if (poolId == null) {
      return QueryUtils.newFilter("executorExpired", "false");
    }
    return QueryUtils.newFilter(
        Map.of(
            String.format("%s.keyword", REMOTE_EXECUTOR_POOL_ID_FIELD_NAME),
            poolId,
            "executorExpired",
            "false"));
  }

  private List<SortCriterion> buildSort() {
    return List.of(new SortCriterion().setField(SORT_BY_FIELD).setOrder(SortOrder.DESCENDING));
  }
}
