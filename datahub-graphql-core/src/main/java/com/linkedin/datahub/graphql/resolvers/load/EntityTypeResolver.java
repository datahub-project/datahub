package com.linkedin.datahub.graphql.resolvers.load;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.linkedin.datahub.graphql.generated.Entity;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.dataloader.DataLoader;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * GraphQL resolver responsible for
 *
 *    1. Retrieving a single input urn.
 *    2. Resolving a single Entity
 *
 *
 */
public class EntityTypeResolver implements DataFetcher<CompletableFuture<Entity>> {

    private static final List<String> IDENTITY_FIELDS = ImmutableList.of("__typename", "urn", "type");
    private final List<com.linkedin.datahub.graphql.types.EntityType<?, ?>> _entityTypes;
    private final Function<DataFetchingEnvironment, Entity> _entityProvider;

    public EntityTypeResolver(
        final List<com.linkedin.datahub.graphql.types.EntityType<?, ?>> entityTypes,
        final Function<DataFetchingEnvironment, Entity> entity
    ) {
        _entityTypes = entityTypes;
        _entityProvider = entity;
    }


    private boolean isOnlySelectingIdentityFields(DataFetchingEnvironment environment) {
        return environment.getField().getSelectionSet().getSelections().stream().filter(selection -> {
            if (!(selection instanceof graphql.language.Field)) {
                return true;
            }
            return !IDENTITY_FIELDS.contains(((graphql.language.Field) selection).getName());
        }).count() == 0;
    }

    @Override
    public CompletableFuture get(DataFetchingEnvironment environment) {
        final Entity resolvedEntity = _entityProvider.apply(environment);
        if (resolvedEntity == null) {
            return CompletableFuture.completedFuture(null);
        }

        final Object javaObject = _entityProvider.apply(environment);

        if (isOnlySelectingIdentityFields(environment)) {
            return CompletableFuture.completedFuture(javaObject);
        }

        final com.linkedin.datahub.graphql.types.EntityType filteredEntity = Iterables.getOnlyElement(_entityTypes.stream()
                .filter(entity -> javaObject.getClass().isAssignableFrom(entity.objectClass()))
                .collect(Collectors.toList()));
        final DataLoader loader = environment.getDataLoaderRegistry().getDataLoader(filteredEntity.name());
        final Object key = filteredEntity.getKeyProvider().apply(resolvedEntity);

        return loader.load(key);
    }
}
