package com.linkedin.datahub.graphql.resolvers.load;

import com.google.common.collect.Iterables;
import com.linkedin.datahub.graphql.generated.Entity;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.dataloader.DataLoader;


/**
 * GraphQL resolver responsible for
 *
 *    1. Retrieving a single input urn.
 *    2. Resolving a single Entity
 *
 *
 */
public class EntityTypeBatchResolver implements DataFetcher<CompletableFuture<List<Entity>>> {

    private final List<com.linkedin.datahub.graphql.types.EntityType<?, ?>> _entityTypes;
    private final Function<DataFetchingEnvironment, List<Entity>> _entitiesProvider;

    public EntityTypeBatchResolver(
        final List<com.linkedin.datahub.graphql.types.EntityType<?, ?>> entityTypes,
        final Function<DataFetchingEnvironment, List<Entity>> entitiesProvider
    ) {
        _entityTypes = entityTypes;
        _entitiesProvider = entitiesProvider;
    }

    @Override
    public CompletableFuture<List<Entity>> get(DataFetchingEnvironment environment) {
        final List<Entity> entities = _entitiesProvider.apply(environment);
        if (entities.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        // Assume all entities are of the same type
        final com.linkedin.datahub.graphql.types.EntityType filteredEntity =
            Iterables.getOnlyElement(_entityTypes.stream()
                .filter(entity -> entities.get(0).getClass().isAssignableFrom(entity.objectClass()))
                .collect(Collectors.toList()));

        final DataLoader loader = environment.getDataLoaderRegistry().getDataLoader(filteredEntity.name());
        List keyList = new ArrayList();
        for (Entity entity : entities) {
            keyList.add(filteredEntity.getKeyProvider().apply(entity));
        }
        return loader.loadMany(keyList);
    }
}
