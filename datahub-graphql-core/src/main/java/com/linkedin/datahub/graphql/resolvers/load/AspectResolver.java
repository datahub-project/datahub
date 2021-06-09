package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.data.Data;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.AspectLoadKey;
import com.linkedin.datahub.graphql.generated.Aspect;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.types.LoadableType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;


/**
 * Generic GraphQL resolver responsible for
 *
 *    1. Retrieving a single input urn.
 *    2. Resolving a single {@link LoadableType}.
 *
 *  Note that this resolver expects that {@link DataLoader}s were registered
 *  for the provided {@link LoadableType} under the name provided by {@link LoadableType#name()}
 *
 */
public class AspectResolver implements DataFetcher<CompletableFuture<Aspect>> {

    public AspectResolver() {
    }

    @Override
    public CompletableFuture<Aspect> get(DataFetchingEnvironment environment) {
        final DataLoader<AspectLoadKey, Aspect> loader = environment.getDataLoaderRegistry().getDataLoader("aspect");
        String fieldName = environment.getField().getName();
        Long version = environment.getArgument("version");
        String urn = ((Entity) environment.getSource()).getUrn();

        Object localContext = environment.getLocalContext();
        // if we have context & the version is 0, we should try to retrieve it from the fetched entity
        if (localContext == null && version == 0 || version == null) {
            if (localContext instanceof Map) {
                DataMap aspect = ((Map<String, DataMap>) localContext).getOrDefault(fieldName, null);
                if (aspect != null) {
                    return CompletableFuture.completedFuture(AspectMapper.map(aspect));
                }
            }
        }

        // if the aspect is not in the cache, we need to fetch it
        return loader.load(new AspectLoadKey(urn, fieldName, version));
        // return loader.load(urn);
    }
}
