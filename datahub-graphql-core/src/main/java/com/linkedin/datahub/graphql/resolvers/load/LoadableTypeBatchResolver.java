package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.datahub.graphql.types.LoadableType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.dataloader.DataLoader;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Generic GraphQL resolver responsible for
 *
 *    1. Retrieving a batch of urns.
 *    2. Resolving a single {@link LoadableType}.
 *
 *  Note that this resolver expects that {@link DataLoader}s were registered
 *  for the provided {@link LoadableType} under the name provided by {@link LoadableType#name()}
 *
 * @param <T> the generated GraphQL POJO corresponding to the resolved type.
 * @param <K> the key type for the DataLoader
 */
public class LoadableTypeBatchResolver<T, K> implements DataFetcher<CompletableFuture<List<T>>> {

    private final LoadableType<T, K> _loadableType;
    private final Function<DataFetchingEnvironment, List<K>> _keyProvider;

    public LoadableTypeBatchResolver(final LoadableType<T, K> loadableType, final Function<DataFetchingEnvironment, List<K>> keyProvider) {
        _loadableType = loadableType;
        _keyProvider = keyProvider;
    }

    @Override
    public CompletableFuture<List<T>> get(DataFetchingEnvironment environment) {
        final List<K> keys = _keyProvider.apply(environment);
        if (keys == null) {
            return null;
        }
        final DataLoader<K, T> loader = environment.getDataLoaderRegistry().getDataLoader(_loadableType.name());
        return loader.loadMany(keys);
    }
}
