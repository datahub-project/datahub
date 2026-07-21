package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ExecutionRequest;
import com.linkedin.datahub.graphql.generated.IngestionSourceExecutionRequests;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

/**
 * Batches {@code IngestionSource.executions(start:0, count:1)} across sources in a GraphQL request
 * into a single {@link EntityClient#filterLatestByValues} call (terms + top_hits).
 */
@Slf4j
@RequiredArgsConstructor
public class LatestIngestionSourceExecutionsBatchLoader {

  public static final String LOADER_NAME = "LatestIngestionSourceExecutions";

  private static final String INGESTION_SOURCE_FIELD_NAME = "ingestionSource";
  private static final String REQUEST_TIME_MS_FIELD_NAME = "requestTimeMs";

  private final EntityClient entityClient;

  public static DataLoader<String, IngestionSourceExecutionRequests> createDataLoader(
      final EntityClient entityClient, final QueryContext queryContext) {
    final LatestIngestionSourceExecutionsBatchLoader loader =
        new LatestIngestionSourceExecutionsBatchLoader(entityClient);
    final BatchLoaderContextProvider provider = () -> queryContext;
    final DataLoaderOptions options =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(provider);
    return DataLoader.newDataLoader(
        (keys, env) ->
            GraphQLConcurrencyUtils.supplyAsync(
                () -> loader.batchLoad(keys, (QueryContext) env.getContext()),
                LOADER_NAME,
                "batchLoad"),
        options);
  }

  public List<IngestionSourceExecutionRequests> batchLoad(
      final List<String> sourceUrns, final QueryContext context) {
    try {
      final Map<String, SearchResult> results =
          entityClient.filterLatestByValues(
              context.getOperationContext(),
              Constants.EXECUTION_REQUEST_ENTITY_NAME,
              INGESTION_SOURCE_FIELD_NAME,
              sourceUrns,
              Collections.singletonList(
                  new SortCriterion()
                      .setField(REQUEST_TIME_MS_FIELD_NAME)
                      .setOrder(SortOrder.DESCENDING)),
              1);

      final List<IngestionSourceExecutionRequests> mapped = new ArrayList<>(sourceUrns.size());
      for (String sourceUrn : sourceUrns) {
        mapped.add(mapResult(results.get(sourceUrn)));
      }
      return mapped;
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load latest ingestion source executions", e);
    }
  }

  private static IngestionSourceExecutionRequests mapResult(final SearchResult searchResult) {
    final IngestionSourceExecutionRequests result = new IngestionSourceExecutionRequests();
    result.setStart(0);
    result.setCount(1);
    if (searchResult == null) {
      result.setTotal(0);
      result.setExecutionRequests(Collections.emptyList());
      return result;
    }
    result.setTotal(searchResult.getNumEntities());
    result.setExecutionRequests(mapUnresolvedExecutionRequests(searchResult.getEntities()));
    return result;
  }

  private static List<ExecutionRequest> mapUnresolvedExecutionRequests(
      final SearchEntityArray entityArray) {
    final List<ExecutionRequest> results = new ArrayList<>();
    if (entityArray == null) {
      return results;
    }
    for (final SearchEntity entity : entityArray) {
      final Urn urn = entity.getEntity();
      final ExecutionRequest executionRequest = new ExecutionRequest();
      executionRequest.setUrn(urn.toString());
      executionRequest.setType(EntityType.EXECUTION_REQUEST);
      results.add(executionRequest);
    }
    return results;
  }
}
