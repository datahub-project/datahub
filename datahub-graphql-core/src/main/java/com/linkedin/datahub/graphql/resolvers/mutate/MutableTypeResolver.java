/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.types.MutableType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic GraphQL resolver responsible for performing updates against particular types.
 *
 * @param <I> the generated GraphQL POJO corresponding to the input type.
 * @param <T> the generated GraphQL POJO corresponding to the return type.
 */
public class MutableTypeResolver<I, T> implements DataFetcher<CompletableFuture<T>> {

  private static final Logger _logger =
      LoggerFactory.getLogger(MutableTypeResolver.class.getName());

  private final MutableType<I, T> _mutableType;

  public MutableTypeResolver(final MutableType<I, T> mutableType) {
    _mutableType = mutableType;
  }

  @Override
  public CompletableFuture<T> get(DataFetchingEnvironment environment) throws Exception {
    final String urn = environment.getArgument("urn");
    final I input = bindArgument(environment.getArgument("input"), _mutableType.inputClass());
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            _logger.debug(String.format("Mutating entity. input: %s", input));
            return _mutableType.update(urn, input, environment.getContext());
          } catch (AuthorizationException e) {
            throw e;
          } catch (Exception e) {
            _logger.error(
                String.format("Failed to perform update against input %s", input)
                    + " "
                    + e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
