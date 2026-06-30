package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.Restricted;
import com.linkedin.datahub.graphql.types.LoadableType;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
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
 * @param <K> the key type for the DataLoader
 */
public class LoadableTypeResolver<T, K> implements DataFetcher<CompletableFuture<T>> {

  private final LoadableType<T, K> _loadableType;
  private final Function<DataFetchingEnvironment, K> _keyProvider;

  public LoadableTypeResolver(
      final LoadableType<T, K> loadableType,
      final Function<DataFetchingEnvironment, K> keyProvider) {
    _loadableType = loadableType;
    _keyProvider = keyProvider;
  }

  @Override
  public CompletableFuture<T> get(DataFetchingEnvironment environment) {
    final K key = _keyProvider.apply(environment);
    if (key == null) {
      return null;
    }
    final DataLoader<K, ?> loader =
        environment.getDataLoaderRegistry().getDataLoader(_loadableType.name());
    return loader.load(key).thenApply(result -> unwrapLoadResult(result, key));
  }

  /**
   * DataLoaders for {@link LoadableType} return {@link DataFetcherResult} values. Unauthorized
   * entities are represented as {@link Restricted} placeholders, which are valid only on the {@code
   * Entity} union (e.g. lineage, search). Typed root fields such as {@code domain(urn): Domain}
   * must not receive a Restricted source object — doing so causes field resolution failures such as
   * "Restricted cannot be cast to Domain".
   */
  @SuppressWarnings("unchecked")
  private T unwrapLoadResult(final Object result, final K key) {
    Object data = result;
    if (result instanceof DataFetcherResult) {
      final DataFetcherResult<?> fetcherResult = (DataFetcherResult<?>) result;
      if (fetcherResult.hasErrors()) {
        throw new AuthorizationException(String.format("Failed to load entity for key %s", key));
      }
      data = fetcherResult.getData();
    }
    if (data instanceof Restricted && !Restricted.class.equals(_loadableType.objectClass())) {
      throw new AuthorizationException(String.format("Unauthorized to view entity: %s", key));
    }
    return (T) data;
  }
}
