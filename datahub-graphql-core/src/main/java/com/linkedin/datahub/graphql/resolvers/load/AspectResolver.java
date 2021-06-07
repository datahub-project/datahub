package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.datahub.graphql.types.LoadableType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
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
 * @param <T> the generated GraphQL POJO corresponding to the resolved type.
 */
public class AspectResolver<T> implements DataFetcher<CompletableFuture<T>> {

    public AspectResolver() {
    }

    @Override
    public CompletableFuture<T> get(DataFetchingEnvironment environment) {
        final DataLoader<String, T> loader = environment.getDataLoaderRegistry().getDataLoader("aspect");
        return loader.load(urn);
    }
}
