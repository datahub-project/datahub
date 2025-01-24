package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.types.BatchMutableType;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic GraphQL resolver responsible for performing updates against particular types.
 *
 * @param <I> the generated GraphQL POJO corresponding to the input type.
 * @param <T> the generated GraphQL POJO corresponding to the return type.
 */
public class MutableTypeBatchResolver<I, B, T> implements DataFetcher<CompletableFuture<List<T>>> {

  private static final Logger _logger =
      LoggerFactory.getLogger(MutableTypeBatchResolver.class.getName());

  private final BatchMutableType<I, B, T> _batchMutableType;

  public MutableTypeBatchResolver(final BatchMutableType<I, B, T> batchMutableType) {
    _batchMutableType = batchMutableType;
  }

  @Override
  public CompletableFuture<List<T>> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final OperationContext opContext = context.getOperationContext();

    final B[] input =
        bindArgument(environment.getArgument("input"), _batchMutableType.batchInputClass());

    return opContext.withSpan(
        "batchMutate",
        () ->
            GraphQLConcurrencyUtils.supplyAsync(
                () -> {
                  try {
                    return _batchMutableType.batchUpdate(input, environment.getContext());
                  } catch (AuthorizationException e) {
                    throw e;
                  } catch (Exception e) {
                    _logger.error("Failed to perform batchUpdate", e);
                    throw new IllegalArgumentException(e);
                  }
                },
                this.getClass().getSimpleName(),
                "get"),
        MetricUtils.DROPWIZARD_METRIC,
        "true");
  }
}
