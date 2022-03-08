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
 */
public class LoadableTypeBatchResolver<T> implements DataFetcher<CompletableFuture<List<T>>> {

    private final LoadableType<T> _loadableType;
    private final Function<DataFetchingEnvironment, List<String>> _urnProvider;

    public LoadableTypeBatchResolver(final LoadableType<T> loadableType, final Function<DataFetchingEnvironment, List<String>> urnProvider) {
        _loadableType = loadableType;
        _urnProvider = urnProvider;
    }

    @Override
    public CompletableFuture<List<T>> get(DataFetchingEnvironment environment) {
        final List<String> urns = _urnProvider.apply(environment);
        if (urns == null) {
            return null;
        }
        final DataLoader<String, T> loader = environment.getDataLoaderRegistry().getDataLoader(_loadableType.name());
        return loader.loadMany(urns);
    }
}
