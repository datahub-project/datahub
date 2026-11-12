package com.linkedin.datahub.graphql.types;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.util.AspectUtils;
import graphql.execution.DataFetcherResult;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * GQL graph type that can be loaded from a downstream service by primary key.
 *
 * @param <T>: The GraphQL object type corresponding to the type.
 * @param <K> the key type for the DataLoader
 */
public interface LoadableType<T, K> {

  /** Returns generated GraphQL class associated with the type */
  Class<T> objectClass();

  /**
   * Returns the name of the type, to be used in creating a corresponding GraphQL {@link
   * org.dataloader.DataLoader}
   */
  default String name() {
    return objectClass().getSimpleName();
  }

  /**
   * Retrieves an entity by urn string. Null is provided in place of an entity object if an entity
   * cannot be found.
   *
   * <p>This path bypasses DataLoader resolvers, so it merges {@code FETCH_ALL} before {@link
   * #batchLoad} to avoid under-fetching when the request already has a narrow aspect selection.
   *
   * @param key to retrieve
   * @param context the {@link QueryContext} corresponding to the request.
   */
  default DataFetcherResult<T> load(@Nonnull final K key, @Nonnull final QueryContext context)
      throws Exception {
    AspectUtils.ensureFetchAllForDirectLoad(context, name());
    return batchLoad(ImmutableList.of(key), context).get(0);
  }
  ;

  /**
   * Retrieves an list of entities given a list of urn strings. The list returned is expected to be
   * of same length of the list of urns, where nulls are provided in place of an entity object if an
   * entity cannot be found.
   *
   * @param keys to retrieve
   * @param context the {@link QueryContext} corresponding to the request.
   */
  List<DataFetcherResult<T>> batchLoad(
      @Nonnull final List<K> keys, @Nonnull final QueryContext context) throws Exception;
}
