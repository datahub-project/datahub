package com.linkedin.datahub.graphql.resolvers.entity;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


/**
 * Resolver responsible for returning whether an entity exists.
 */
public class EntityExistsResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService _entityService;

  public EntityExistsResolver(final EntityService entityService) {
    _entityService = entityService;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {
    final String entityUrnString = bindArgument(environment.getArgument("urn"), String.class);
    Objects.requireNonNull(entityUrnString, "Entity urn must not be null!");

    Urn entityUrn = Urn.createFromString(entityUrnString);
    return CompletableFuture.supplyAsync(() -> {
      try {
        return _entityService.exists(entityUrn);
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to check whether entity %s exists", entityUrnString));
      }
    });
  }
}