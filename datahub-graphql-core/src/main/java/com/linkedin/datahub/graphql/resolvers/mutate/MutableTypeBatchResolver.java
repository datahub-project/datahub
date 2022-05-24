package com.linkedin.datahub.graphql.resolvers.mutate;

import com.codahale.metrics.Timer;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.types.MutableType;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


/**
 * Generic GraphQL resolver responsible for performing updates against particular types.
 *
 * @param <I> the generated GraphQL POJO corresponding to the input type.
 * @param <T> the generated GraphQL POJO corresponding to the return type.
 */
public class MutableTypeBatchResolver<I, T> implements DataFetcher<CompletableFuture<T>> {

  private static final Logger _logger = LoggerFactory.getLogger(MutableTypeBatchResolver.class.getName());

  private final MutableType<I, T> _mutableType;

  public MutableTypeBatchResolver(final MutableType<I, T> mutableType) {
    _mutableType = mutableType;
  }

  @Override
  public CompletableFuture<T> get(DataFetchingEnvironment environment) throws Exception {
    final I[] inputs = bindArgument(environment.getArgument("inputs"), _mutableType.arrayInputClass());

    return CompletableFuture.supplyAsync(() -> {
      Timer.Context timer = MetricUtils.timer(this.getClass(), "batchMutate").time();

      try {
        return _mutableType.batchUpdate(inputs, environment.getContext());
      } catch (AuthorizationException e) {
        throw e;
      } catch (Exception e) {
        _logger.error("Failed to perform batchUpdate", e);
        throw new IllegalArgumentException(e);
      } finally {
        timer.stop();
      }
    });
  }
}
