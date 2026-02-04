package com.linkedin.datahub.graphql.types;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.types.common.mappers.RestrictedResultMapper;
import com.linkedin.entity.EntityResponse;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
   * Converts a key to a URN for authorization checks. Default implementation assumes K is a String
   * URN. Override this method for non-String key types (e.g., VersionedAspectKey).
   *
   * @param key the key to convert
   * @return Function that converts K to Urn
   */
  default Function<K, Urn> getKeyToUrn() {
    return key -> UrnUtils.getUrn((String) key);
  }

  /**
   * Returns the RestrictedService for encrypting URNs. Override this method to provide the
   * RestrictedService instance. Returns null by default.
   *
   * @return RestrictedService or null
   */
  @Nullable
  default RestrictedService getRestrictedService() {
    return null;
  }

  /**
   * Creates a Restricted entity result for unauthorized access.
   *
   * @param urn the URN that the user cannot view
   * @return DataFetcherResult containing a Restricted entity
   */
  default DataFetcherResult<T> createRestrictedResult(@Nonnull Urn urn) {
    return RestrictedResultMapper.createRestrictedResult(urn, getRestrictedService());
  }

  /**
   * Maps EntityResponse objects to DataFetcherResults, preserving order of input URNs. Returns null
   * for URNs that don't have a corresponding response.
   *
   * @param urnStrs list of URN strings in the order results should be returned
   * @param responseMap map of URN to EntityResponse from the backend
   * @param mapper function to convert (QueryContext, EntityResponse) to the entity type T
   * @param context the query context
   * @return list of DataFetcherResults in same order as urnStrs
   */
  default List<DataFetcherResult<T>> mapResponsesToBatchResults(
      @Nonnull List<String> urnStrs,
      @Nonnull Map<Urn, EntityResponse> responseMap,
      @Nonnull BiFunction<QueryContext, EntityResponse, T> mapper,
      @Nonnull QueryContext context) {
    return urnStrs.stream()
        .map(
            urnStr -> {
              Urn urn = UrnUtils.getUrn(urnStr);
              EntityResponse response = responseMap.get(urn);
              if (response == null) {
                return null;
              }
              return DataFetcherResult.<T>newResult().data(mapper.apply(context, response)).build();
            })
        .collect(Collectors.toList());
  }

  /**
   * Retrieves an entity by urn string. Null is provided in place of an entity object if an entity
   * cannot be found.
   *
   * @param key to retrieve
   * @param context the {@link QueryContext} corresponding to the request.
   */
  default DataFetcherResult<T> load(@Nonnull final K key, @Nonnull final QueryContext context)
      throws Exception {
    return batchLoad(ImmutableList.of(key), context).get(0);
  }

  /**
   * Retrieves a list of entities with authorization checks. Unauthorized entities are returned as
   * Restricted entities. The list returned is the same length as the input keys.
   *
   * <p>This default implementation: 1. Checks authorization for each key 2. Only fetches authorized
   * entities via batchLoadWithoutAuthorization 3. Returns Restricted entities for unauthorized keys
   *
   * @param keys to retrieve
   * @param context the {@link QueryContext} corresponding to the request.
   */
  default List<DataFetcherResult<T>> batchLoad(
      @Nonnull final List<K> keys, @Nonnull final QueryContext context) throws Exception {

    Function<K, Urn> keyToUrn = getKeyToUrn();

    // 1. Check authorization for each unique key
    Map<K, Boolean> isAuthorized =
        keys.stream()
            .distinct()
            .collect(
                Collectors.toMap(
                    k -> k, k -> canView(context.getOperationContext(), keyToUrn.apply(k))));

    // 2. Get authorized keys only
    List<K> authorizedKeys =
        keys.stream().filter(isAuthorized::get).distinct().collect(Collectors.toList());

    // 3. Fetch only authorized entities
    List<DataFetcherResult<T>> rawResults = batchLoadWithoutAuthorization(authorizedKeys, context);

    // 4. Build lookup map from results
    Map<K, DataFetcherResult<T>> resultsByKey = new HashMap<>();
    for (int i = 0; i < authorizedKeys.size(); i++) {
      resultsByKey.put(authorizedKeys.get(i), rawResults.get(i));
    }

    // 5. Reconstruct results in original order
    return keys.stream()
        .map(
            k -> {
              if (!isAuthorized.get(k)) {
                return createRestrictedResult(keyToUrn.apply(k));
              }
              return resultsByKey.get(k); // May be null if entity doesn't exist
            })
        .collect(Collectors.toList());
  }

  /**
   * Retrieves a list of entities WITHOUT authorization checks. This method should only fetch data
   * and map to entities - authorization is handled by the default batchLoad wrapper.
   *
   * <p>The list returned is expected to be of same length as the list of keys, where nulls are
   * provided in place of an entity object if an entity cannot be found.
   *
   * @param keys to retrieve (already filtered to authorized keys)
   * @param context the {@link QueryContext} corresponding to the request.
   */
  List<DataFetcherResult<T>> batchLoadWithoutAuthorization(
      @Nonnull final List<K> keys, @Nonnull final QueryContext context) throws Exception;
}
