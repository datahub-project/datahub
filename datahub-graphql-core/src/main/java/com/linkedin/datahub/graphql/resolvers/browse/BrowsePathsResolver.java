/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.browse;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowsePathsInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrowsePathsResolver implements DataFetcher<CompletableFuture<List<BrowsePath>>> {

  private static final Logger _logger =
      LoggerFactory.getLogger(BrowsePathsResolver.class.getName());

  private final Map<EntityType, BrowsableEntityType<?, ?>> _typeToEntity;

  public BrowsePathsResolver(@Nonnull final List<BrowsableEntityType<?, ?>> browsableEntities) {
    _typeToEntity =
        browsableEntities.stream()
            .collect(Collectors.toMap(BrowsableEntityType::type, entity -> entity));
  }

  @Override
  public CompletableFuture<List<BrowsePath>> get(DataFetchingEnvironment environment) {
    final BrowsePathsInput input =
        bindArgument(environment.getArgument("input"), BrowsePathsInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            _logger.debug(
                String.format(
                    "Fetch browse paths. entity type: %s, urn: %s",
                    input.getType(), input.getUrn()));
            if (_typeToEntity.containsKey(input.getType())) {
              return _typeToEntity
                  .get(input.getType())
                  .browsePaths(input.getUrn(), environment.getContext());
            }
            // Browse path is impl detail.
            return Collections.emptyList();
          } catch (Exception e) {
            _logger.error(
                "Failed to retrieve browse paths: "
                    + String.format("entity type %s, urn %s", input.getType(), input.getUrn())
                    + " "
                    + e.getMessage());
            throw new RuntimeException(
                "Failed to retrieve browse paths: "
                    + String.format("entity type %s, urn %s", input.getType(), input.getUrn()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
