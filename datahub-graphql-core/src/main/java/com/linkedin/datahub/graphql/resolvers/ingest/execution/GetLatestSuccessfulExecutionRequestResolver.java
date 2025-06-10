package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.ExecutionRequest;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.ingestion.ExecutionRequestType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

/** Returns the latest successful execution request by ingestion source */
public class GetLatestSuccessfulExecutionRequestResolver
    implements DataFetcher<CompletableFuture<ExecutionRequest>> {

  private final EntityClient _entityClient;
  private final ExecutionRequestType _executionRequestType;

  private final String INGESTION_SOURCE_FIELD = "ingestionSource";
  private final String RESULT_STATUS_FIELD = "executionResultStatus";
  private final String RESULT_REQUEST_TIME_MS_FIELD = "requestTimeMs";
  private final String STATUS_SUCCESS = "SUCCESS";

  public GetLatestSuccessfulExecutionRequestResolver(
      final EntityClient entityClient, final ExecutionRequestType executionRequestType) {
    _entityClient = entityClient;
    _executionRequestType = executionRequestType;
  }

  @Override
  public CompletableFuture<ExecutionRequest> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final String ingestionSourceUrn = ((IngestionSource) environment.getSource()).getUrn();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Urn executionRequestUrn =
                getLatestSuccessfulExecutionRequestUrn(ingestionSourceUrn, context);

            if (executionRequestUrn == null) {
              return null;
            }

            Optional<DataFetcherResult<ExecutionRequest>> resp =
                _executionRequestType
                    .batchLoad(List.of(executionRequestUrn.toString()), context)
                    .stream()
                    .findFirst();
            return resp.map(DataFetcherResult::getData).orElse(null);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to get latest successful execution request by source: %s",
                    ingestionSourceUrn),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private Urn getLatestSuccessfulExecutionRequestUrn(
      @Nonnull final String ingestionSourceUrn, @Nonnull QueryContext context) throws Exception {
    final SearchResult gmsResult =
        _entityClient.filter(
            context.getOperationContext(),
            Constants.EXECUTION_REQUEST_ENTITY_NAME,
            Objects.requireNonNull(
                ResolverUtils.buildFilter(
                    List.of(
                        new FacetFilterInput(
                            INGESTION_SOURCE_FIELD,
                            null,
                            List.of(ingestionSourceUrn),
                            false,
                            FilterOperator.EQUAL),
                        new FacetFilterInput(
                            RESULT_STATUS_FIELD,
                            null,
                            List.of(STATUS_SUCCESS),
                            false,
                            FilterOperator.EQUAL)),
                    Collections.emptyList())),
            Collections.singletonList(
                new SortCriterion()
                    .setField(RESULT_REQUEST_TIME_MS_FIELD)
                    .setOrder(SortOrder.DESCENDING)),
            0,
            1);
    Optional<SearchEntity> optionalSearchEntity = gmsResult.getEntities().stream().findFirst();

    return optionalSearchEntity.map(SearchEntity::getEntity).orElse(null);
  }
}
