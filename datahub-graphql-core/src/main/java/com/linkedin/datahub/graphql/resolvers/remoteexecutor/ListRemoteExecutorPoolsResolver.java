package com.linkedin.datahub.graphql.resolvers.remoteexecutor;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.AcrylConstants.*;

import com.google.common.collect.Iterables;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ListRemoteExecutorPoolsInput;
import com.linkedin.datahub.graphql.generated.ListRemoteExecutorPoolsResult;
import com.linkedin.datahub.graphql.generated.RemoteExecutorPool;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.executorpool.RemoteExecutorPoolInfo;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ListRemoteExecutorPoolsResolver
    implements DataFetcher<CompletableFuture<ListRemoteExecutorPoolsResult>> {
  final EntityClient _entityClient;
  private static final String SORT_BY_FIELD =
      Iterables.getLast(RemoteExecutorPoolInfo.fields().createdAt().getPathComponents());

  @Override
  public CompletableFuture<ListRemoteExecutorPoolsResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final ListRemoteExecutorPoolsInput input =
        bindArgument(environment.getArgument("input"), ListRemoteExecutorPoolsInput.class);
    final int start = input.getStart();
    final int count = input.getCount();
    final String maybeQuery = input.getQuery();
    final String query = maybeQuery == null ? "*" : maybeQuery;

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final ListRemoteExecutorPoolsResult result = new ListRemoteExecutorPoolsResult();
            result.setStart(start);

            // 1. List executor urns in an ordered manner
            final SearchResult searchResult =
                searchExecutorPools(context.getOperationContext(), query, start, count);

            final List<Urn> executorUrns =
                searchResult.getEntities().stream().map(SearchEntity::getEntity).toList();
            result.setTotal(searchResult.getNumEntities());

            // If no results, return early
            if (executorUrns.isEmpty()) {
              result.setRemoteExecutorPools(Collections.emptyList());
              result.setCount(0);
              return result;
            }

            // 2. Map the executors for the retrieved urns, so gql can use the type resolver to load
            // them in
            final List<RemoteExecutorPool> executors =
                executorUrns.stream()
                    .map(
                        urn -> {
                          final RemoteExecutorPool executor = new RemoteExecutorPool();
                          executor.setUrn(urn.toString());
                          executor.setType(EntityType.REMOTE_EXECUTOR_POOL);
                          return executor;
                        })
                    .toList();

            // 3. Return the result
            result.setRemoteExecutorPools(executors);
            result.setCount(executors.size());
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list remote executor pools", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  @Nonnull
  private SearchResult searchExecutorPools(
      @Nonnull OperationContext operationContext, @Nonnull String query, int start, int count)
      throws RemoteInvocationException {
    return this._entityClient.search(
        operationContext,
        REMOTE_EXECUTOR_POOL_ENTITY_NAME,
        query,
        new Filter().setOr(new ConjunctiveCriterionArray()),
        buildSort(),
        start,
        count);
  }

  private List<SortCriterion> buildSort() {
    return List.of(new SortCriterion().setField(SORT_BY_FIELD).setOrder(SortOrder.DESCENDING));
  }
}
