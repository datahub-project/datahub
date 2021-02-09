package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.datahub.graphql.types.LoadableType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.dataloader.DataLoader;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

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
public class LoadableTypeResolver<T> implements DataFetcher<CompletableFuture<T>> {

    private final LoadableType<T> _loadableType;
    private final Function<DataFetchingEnvironment, String> _urnProvider;

    public LoadableTypeResolver(final LoadableType<T> loadableType, final Function<DataFetchingEnvironment, String> urnProvider) {
        _loadableType = loadableType;
        _urnProvider = urnProvider;
    }

    @Override
    public CompletableFuture<T> get(DataFetchingEnvironment environment) {
        final String urn = _urnProvider.apply(environment);
        final DataLoader<String, T> loader = environment.getDataLoaderRegistry().getDataLoader(_loadableType.name());
        return loader.load(urn);
    }
}
