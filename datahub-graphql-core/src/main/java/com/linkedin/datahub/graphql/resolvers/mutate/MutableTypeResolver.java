package com.linkedin.datahub.graphql.resolvers.mutate;

import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.types.MutableType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

/**
 * Generic GraphQL resolver responsible for performing updates against particular types.
 *
 * @param <I> the generated GraphQL POJO corresponding to the input type.
 * @param <T> the generated GraphQL POJO corresponding to the return type.
 */
public class MutableTypeResolver<I, T> implements DataFetcher<CompletableFuture<T>> {

    private static final Logger _logger = LoggerFactory.getLogger(MutableTypeResolver.class.getName());

    private final MutableType<I, T> _mutableType;

    public MutableTypeResolver(final MutableType<I, T> mutableType) {
        _mutableType = mutableType;
    }

    @Override
    public CompletableFuture<T> get(DataFetchingEnvironment environment) throws Exception {
        final String urn = environment.getArgument("urn");
        final I input = bindArgument(environment.getArgument("input"), _mutableType.inputClass());
        return CompletableFuture.supplyAsync(() -> {
            try {
                _logger.debug(String.format("Mutating entity. input: %s", input));
                return _mutableType.update(urn, input, environment.getContext());
            } catch (AuthorizationException e) {
                throw e;
            } catch (Exception e) {
                _logger.error(String.format("Failed to perform update against input %s", input) + " " + e.getMessage());
                throw new RuntimeException(String.format("Failed to perform update against input %s", input), e);
            }
        });
    }
}
