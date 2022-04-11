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
 *    1. Generating a single input AspectLoadKey.
 *    2. Resolving a single {@link Aspect}.
 *
 */
public class AspectResolver implements DataFetcher<CompletableFuture<Aspect>> {
    private final String fieldNameOverride;

    public AspectResolver() {
        fieldNameOverride = null;
    }

    public AspectResolver(String fieldNameOverride) {
        this.fieldNameOverride = fieldNameOverride;
    }

    @Override
    public CompletableFuture<Aspect> get(DataFetchingEnvironment environment) {
        final DataLoader<VersionedAspectKey, Aspect> loader = environment.getDataLoaderRegistry().getDataLoader("Aspect");
        final String fieldName = fieldNameOverride != null ? fieldNameOverride : environment.getField().getName();
        final Long version = environment.containsArgument("version") ? environment.getArgument("version") : 0L;
        final String urn = ((Entity) environment.getSource()).getUrn();
        return loader.load(new VersionedAspectKey(urn, fieldName, version));
    }
}
