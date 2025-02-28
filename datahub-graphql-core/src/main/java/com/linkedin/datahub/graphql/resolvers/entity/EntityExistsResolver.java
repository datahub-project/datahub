package com.linkedin.datahub.graphql.resolvers.entity;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Resolver responsible for returning whether an entity exists. */
public class EntityExistsResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService<?> _entityService;

  public EntityExistsResolver(final EntityService<?> entityService) {
    _entityService = entityService;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    String entityUrnString = bindArgument(environment.getArgument("urn"), String.class);
    // resolver can be used as its own endpoint or when hydrating an entity
    if (entityUrnString == null && environment.getSource() != null) {
      entityUrnString = ((Entity) environment.getSource()).getUrn();
    }
    Objects.requireNonNull(entityUrnString, "Entity urn must not be null!");

    final Urn entityUrn = Urn.createFromString(entityUrnString);
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return _entityService
                .exists(context.getOperationContext(), Set.of(entityUrn))
                .contains(entityUrn);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to check whether entity %s exists", entityUrn.toString()));
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
