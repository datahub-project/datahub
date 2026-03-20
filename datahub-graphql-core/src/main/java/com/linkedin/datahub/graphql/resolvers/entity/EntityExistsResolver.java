package com.linkedin.datahub.graphql.resolvers.entity;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;

/** Resolver responsible for returning whether an entity exists. */
public class EntityExistsResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService<?> _entityService;

  public EntityExistsResolver(final EntityService<?> entityService) {
    _entityService = entityService;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    String entityUrnString = bindArgument(environment.getArgument("urn"), String.class);
    // resolver can be used as its own endpoint or when hydrating an entity
    if (entityUrnString == null && environment.getSource() != null) {
      entityUrnString = ((Entity) environment.getSource()).getUrn();
    }
    Objects.requireNonNull(entityUrnString, "Entity urn must not be null!");

    // Use DataLoader to batch multiple existence checks into a single query
    final DataLoader<String, Boolean> dataLoader =
        environment.getDataLoaderRegistry().getDataLoader("ENTITY_EXISTS");
    return dataLoader.load(entityUrnString);
  }
}
