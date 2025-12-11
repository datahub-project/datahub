/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthenticationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

/**
 * Checks whether the user is currently authenticated & if so delegates execution to a child
 * resolver.
 */
@Deprecated
public final class AuthenticatedResolver<T> implements DataFetcher<T> {

  private final DataFetcher<T> _resolver;

  public AuthenticatedResolver(final DataFetcher<T> resolver) {
    _resolver = resolver;
  }

  @Override
  public final T get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    if (context.isAuthenticated()) {
      return _resolver.get(environment);
    }
    throw new AuthenticationException("Failed to authenticate the current user.");
  }
}
