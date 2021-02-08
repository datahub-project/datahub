package com.linkedin.datahub.graphql.resolvers.write;

import com.linkedin.datahub.graphql.types.WritableType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

/**
 * Generic GraphQL resolver responsible for performing writes against particular types.
 *
 * @param <T> the generated GraphQL POJO corresponding to the resolved type.
 */
public class WritableTypeResolver<I, T> implements DataFetcher<CompletableFuture<T>> {

    private final WritableType<I> _writableType;

    public WritableTypeResolver(final WritableType<I> writableType) {
        _writableType = writableType;
    }

    @Override
    public CompletableFuture<T> get(DataFetchingEnvironment environment) throws Exception {
        final I input = bindArgument(environment.getArgument("input"), _writableType.inputClass());
        return CompletableFuture.supplyAsync(() -> {
            try {
                return _writableType.update(input, environment.getContext());
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
            }
        });
    }
}
