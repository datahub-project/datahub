/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.browse;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

public class EntityBrowsePathsResolver implements DataFetcher<CompletableFuture<List<BrowsePath>>> {

  private final BrowsableEntityType<?, ?> _browsableType;

  public EntityBrowsePathsResolver(@Nonnull final BrowsableEntityType<?, ?> browsableType) {
    _browsableType = browsableType;
  }

  @Override
  public CompletableFuture<List<BrowsePath>> get(DataFetchingEnvironment environment) {

    final QueryContext context = environment.getContext();
    final String urn = ((Entity) environment.getSource()).getUrn();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return _browsableType.browsePaths(urn, context);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to retrieve browse paths for entity with urn %s", urn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
