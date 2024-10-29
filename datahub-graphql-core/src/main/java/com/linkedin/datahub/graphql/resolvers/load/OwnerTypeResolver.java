package com.linkedin.datahub.graphql.resolvers.load;

import com.google.common.collect.Iterables;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.OwnerType;
import com.linkedin.datahub.graphql.types.LoadableType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.dataloader.DataLoader;

/**
 * Generic GraphQL resolver responsible for
 *
 * <p>1. Retrieving a single input urn. 2. Resolving a single {@link LoadableType}.
 *
 * <p>Note that this resolver expects that {@link DataLoader}s were registered for the provided
 * {@link LoadableType} under the name provided by {@link LoadableType#name()}
 *
 * @param <T> the generated GraphQL POJO corresponding to the resolved type.
 */
public class OwnerTypeResolver<T> implements DataFetcher<CompletableFuture<T>> {

  private final List<LoadableType<?, ?>> _loadableTypes;
  private final Function<DataFetchingEnvironment, OwnerType> _urnProvider;

  public OwnerTypeResolver(
      final List<LoadableType<?, ?>> loadableTypes,
      final Function<DataFetchingEnvironment, OwnerType> urnProvider) {
    _loadableTypes = loadableTypes;
    _urnProvider = urnProvider;
  }

  @Override
  public CompletableFuture<T> get(DataFetchingEnvironment environment) {
    final OwnerType ownerType = _urnProvider.apply(environment);
    final LoadableType<?, ?> filteredEntity =
        Iterables.getOnlyElement(
            _loadableTypes.stream()
                .filter(entity -> ownerType.getClass().isAssignableFrom(entity.objectClass()))
                .collect(Collectors.toList()));
    final DataLoader<String, T> loader =
        environment.getDataLoaderRegistry().getDataLoader(filteredEntity.name());
    return loader.load(((Entity) ownerType).getUrn());
  }
}
