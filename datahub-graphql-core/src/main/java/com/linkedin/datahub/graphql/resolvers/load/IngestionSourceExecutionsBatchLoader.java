package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ExecutionRequest;
import com.linkedin.datahub.graphql.generated.IngestionSourceExecutionRequests;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

/**
 * Batches {@code IngestionSource.executions(start: 0, count: 1)} lookups across a GraphQL page so
 * list views collapse N ES filters into one {@code searchLatestPerGroup} call.
 */
@Slf4j
@RequiredArgsConstructor
public class IngestionSourceExecutionsBatchLoader {

  public static final String LOADER_NAME = "IngestionSourceExecutions";

  private static final String INGESTION_SOURCE_FIELD_NAME = "ingestionSource";
  private static final String REQUEST_TIME_MS_FIELD_NAME = "requestTimeMs";

  private final EntityClient entityClient;

  public static DataLoader<String, IngestionSourceExecutionRequests> createDataLoader(
      final EntityClient entityClient, final QueryContext queryContext) {
    final IngestionSourceExecutionsBatchLoader loader =
        new IngestionSourceExecutionsBatchLoader(entityClient);
    final BatchLoaderContextProvider contextProvider = () -> queryContext;
    final DataLoaderOptions loaderOptions =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(contextProvider);
    return DataLoader.newDataLoader(
        (keys, env) ->
            GraphQLConcurrencyUtils.supplyAsync(
                () -> loader.batchLoad(keys, (QueryContext) env.getContext()),
                LOADER_NAME,
                "batchLoad"),
        loaderOptions);
  }

  @Nonnull
  public List<IngestionSourceExecutionRequests> batchLoad(
      @Nonnull final List<String> sourceUrns, @Nonnull final QueryContext context) {
    if (sourceUrns.isEmpty()) {
      return Collections.emptyList();
    }

    try {
      if (sourceUrns.size() == 1) {
        // Single source (e.g. detail page asking for latest only) — no aggregation benefit.
        return Collections.singletonList(loadOne(sourceUrns.get(0), context));
      }

      final List<SortCriterion> sortCriteria =
          Collections.singletonList(
              new SortCriterion()
                  .setField(REQUEST_TIME_MS_FIELD_NAME)
                  .setOrder(SortOrder.DESCENDING));
      final Map<String, SearchResult> bySource =
          entityClient.searchLatestPerGroup(
              context.getOperationContext(),
              Constants.EXECUTION_REQUEST_ENTITY_NAME,
              INGESTION_SOURCE_FIELD_NAME,
              sourceUrns,
              sortCriteria);

      return sourceUrns.stream()
          .map(sourceUrn -> mapResult(bySource.getOrDefault(sourceUrn, emptySearchResult())))
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch resolve latest executions for %d ingestion sources",
              sourceUrns.size()),
          e);
    }
  }

  private IngestionSourceExecutionRequests loadOne(
      @Nonnull final String sourceUrn, @Nonnull final QueryContext context)
      throws RemoteInvocationException {
    final SearchResult searchResult =
        entityClient.filter(
            context.getOperationContext(),
            Constants.EXECUTION_REQUEST_ENTITY_NAME,
            new Filter()
                .setOr(
                    new ConjunctiveCriterionArray(
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(
                                        buildCriterion(
                                            INGESTION_SOURCE_FIELD_NAME,
                                            Condition.EQUAL,
                                            sourceUrn)))))),
            Collections.singletonList(
                new SortCriterion()
                    .setField(REQUEST_TIME_MS_FIELD_NAME)
                    .setOrder(SortOrder.DESCENDING)),
            0,
            1);
    return mapResult(searchResult);
  }

  @Nonnull
  private static IngestionSourceExecutionRequests mapResult(
      @Nonnull final SearchResult searchResult) {
    final IngestionSourceExecutionRequests result = new IngestionSourceExecutionRequests();
    result.setStart(searchResult.getFrom());
    result.setCount(searchResult.getPageSize());
    result.setTotal(searchResult.getNumEntities());
    result.setExecutionRequests(mapUnresolvedExecutionRequests(searchResult.getEntities()));
    return result;
  }

  @Nonnull
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

  @Nonnull
  private static SearchResult emptySearchResult() {
    return new SearchResult()
        .setFrom(0)
        .setPageSize(1)
        .setNumEntities(0)
        .setEntities(new SearchEntityArray());
  }
}
