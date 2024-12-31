package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.datahub.graphql.VersionedAspectKey;
import com.linkedin.datahub.graphql.generated.Aspect;
import com.linkedin.datahub.graphql.generated.Entity;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;

/**
 * Generic GraphQL resolver responsible for
 *
 * <p>1. Generating a single input AspectLoadKey. 2. Resolving a single {@link Aspect}.
 */
public class AspectResolver implements DataFetcher<CompletableFuture<Aspect>> {
  @Override
  public CompletableFuture<Aspect> get(DataFetchingEnvironment environment) {
    final DataLoader<VersionedAspectKey, Aspect> loader =
        environment.getDataLoaderRegistry().getDataLoader("Aspect");
    final String fieldName = environment.getField().getName();
    final Long version = environment.getArgument("version");
    final String urn = ((Entity) environment.getSource()).getUrn();
    return loader.load(new VersionedAspectKey(urn, fieldName, version));
  }
}
