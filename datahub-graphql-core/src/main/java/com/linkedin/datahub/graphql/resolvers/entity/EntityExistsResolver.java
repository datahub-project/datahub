package com.linkedin.datahub.graphql.resolvers.entity;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.loaders.EntityExistsBatchLoader;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.dataloader.DataLoader;

/** Resolver responsible for returning whether an entity exists. */
public class EntityExistsResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService<?> _entityService;

  // Null when constructed without feature flags (legacy/test path) — treated as "batch disabled".
  @Nullable private final FeatureFlags _featureFlags;

  /** Test-only: no feature flags means the batch path stays off. */
  EntityExistsResolver(final EntityService<?> entityService) {
    this(entityService, null);
  }

  public EntityExistsResolver(
      final EntityService<?> entityService, @Nullable final FeatureFlags featureFlags) {
    _entityService = entityService;
    _featureFlags = featureFlags;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final String urnArgument = bindArgument(environment.getArgument("urn"), String.class);
    String entityUrnString = urnArgument;
    // resolver can be used as its own endpoint or when hydrating an entity
    if (entityUrnString == null && environment.getSource() != null) {
      entityUrnString = ((Entity) environment.getSource()).getUrn();
    }
    Objects.requireNonNull(entityUrnString, "Entity urn must not be null!");

    final Urn entityUrn = Urn.createFromString(entityUrnString);

    // Only batch when hydrating an entity. A caller-supplied urn keeps its own call so a bad urn
    // cannot fail the batch.
    if (urnArgument == null
        && _featureFlags != null
        && _featureFlags.isEntityExistsBatchLoadEnabled()) {
      final DataLoader<Urn, Boolean> loader =
          environment.getDataLoaderRegistry().getDataLoader(EntityExistsBatchLoader.LOADER_NAME);
      return loader.load(entityUrn);
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return _entityService
                .exists(context.getOperationContext(), Set.of(entityUrn))
                .contains(entityUrn);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to check whether entity %s exists", entityUrn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
