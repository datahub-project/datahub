package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.generated.IngestionSourceExecutionRequests;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionResolverUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Retrieves a list of historical executions for a particular source. */
@Slf4j
public class IngestionSourceExecutionRequestsResolver
    implements DataFetcher<CompletableFuture<IngestionSourceExecutionRequests>> {

  private static final String INGESTION_SOURCE_FIELD_NAME = "ingestionSource";
  private static final String REQUEST_TIME_MS_FIELD_NAME = "requestTimeMs";

  private final EntityClient _entityClient;

  public IngestionSourceExecutionRequestsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<IngestionSourceExecutionRequests> get(
      final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final String urn = ((IngestionSource) environment.getSource()).getUrn();
    final Integer start =
        environment.getArgument("start") != null ? environment.getArgument("start") : 0;
    final Integer count =
        environment.getArgument("count") != null ? environment.getArgument("count") : 10;

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {

            // 1. Fetch the related edges
            final Criterion filterCriterion =
                buildCriterion(INGESTION_SOURCE_FIELD_NAME, Condition.EQUAL, urn);

            final SearchResult executionsSearchResult =
                _entityClient.filter(
                    context.getOperationContext(),
                    Constants.EXECUTION_REQUEST_ENTITY_NAME,
                    new Filter()
                        .setOr(
                            new ConjunctiveCriterionArray(
                                new ConjunctiveCriterion()
                                    .setAnd(
                                        new CriterionArray(ImmutableList.of(filterCriterion))))),
                    Collections.singletonList(
                        new SortCriterion()
                            .setField(REQUEST_TIME_MS_FIELD_NAME)
                            .setOrder(SortOrder.DESCENDING)),
                    start,
                    count);

            // 2. Batch fetch the related ExecutionRequests
            final Set<Urn> relatedExecRequests =
                executionsSearchResult.getEntities().stream()
                    .map(SearchEntity::getEntity)
                    .collect(Collectors.toSet());

            final Map<Urn, EntityResponse> entities =
                _entityClient.batchGetV2(
                    context.getOperationContext(),
                    Constants.EXECUTION_REQUEST_ENTITY_NAME,
                    relatedExecRequests,
                    ImmutableSet.of(
                        Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                        Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME));

            // 3. Map the GMS ExecutionRequests into GraphQL Execution Requests
            final IngestionSourceExecutionRequests result = new IngestionSourceExecutionRequests();
            result.setStart(executionsSearchResult.getFrom());
            result.setCount(executionsSearchResult.getPageSize());
            result.setTotal(executionsSearchResult.getNumEntities());
            result.setExecutionRequests(
                IngestionResolverUtils.mapExecutionRequests(
                    context,
                    executionsSearchResult.getEntities().stream()
                        .map(searchResult -> entities.get(searchResult.getEntity()))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList())));
            return result;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to resolve executions associated with ingestion source with urn %s",
                    urn),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
