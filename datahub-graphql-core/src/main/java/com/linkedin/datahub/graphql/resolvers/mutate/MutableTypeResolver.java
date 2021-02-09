package com.linkedin.datahub.graphql.resolvers.mutate;

import com.linkedin.datahub.graphql.types.MutableType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

/**
 * Generic GraphQL resolver responsible for performing updates against particular types.
 *
 * @param <I> the generated GraphQL POJO corresponding to the input type.
 * @param <T> the generated GraphQL POJO corresponding to the return type.
 */
public class MutableTypeResolver<I, T> implements DataFetcher<CompletableFuture<T>> {

    private final MutableType<I> _mutableType;

    public MutableTypeResolver(final MutableType<I> mutableType) {
        _mutableType = mutableType;
    }

    @Override
    public CompletableFuture<T> get(DataFetchingEnvironment environment) throws Exception {
        final I input = bindArgument(environment.getArgument("input"), _mutableType.inputClass());
        return CompletableFuture.supplyAsync(() -> {
            try {
                return _mutableType.update(input, environment.getContext());
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
            }
        });
    }
}
