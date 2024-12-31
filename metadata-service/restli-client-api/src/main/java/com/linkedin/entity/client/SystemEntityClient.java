package com.linkedin.entity.client;

import com.google.common.cache.Cache;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Adds entity/aspect cache and assumes **system** authentication */
public interface SystemEntityClient extends EntityClient {

  EntityClientCache getEntityClientCache();

  @Nonnull
  Cache<String, OperationContext> getOperationContextMap();

  /**
   * Builds the cache
   *
   * @param cacheConfig cache configuration
   * @return the cache
   */
  default EntityClientCache buildEntityClientCache(
      Class<?> metricClazz, EntityClientCacheConfig cacheConfig) {
    return EntityClientCache.builder()
        .config(cacheConfig)
        .build(
            (EntityClientCache.CollectionKey collectionKey) -> {
              try {
                if (collectionKey != null && !collectionKey.getUrns().isEmpty()) {
                  String entityName =
                      collectionKey.getUrns().stream().findFirst().map(Urn::getEntityType).get();

                  if (collectionKey.getUrns().stream()
                      .anyMatch(urn -> !urn.getEntityType().equals(entityName))) {
                    throw new IllegalArgumentException(
                        "Urns must be of the same entity type. RestliEntityClient API limitation.");
                  }

                  return batchGetV2NoCache(
                      getOperationContextMap().getIfPresent(collectionKey.getContextId()),
                      entityName,
                      collectionKey.getUrns(),
                      collectionKey.getAspectNames());
                } else {
                  return Map.of();
                }
              } catch (RemoteInvocationException | URISyntaxException e) {
                throw new RuntimeException(e);
              }
            },
            metricClazz);
  }

  /**
   * Get an entity by urn with the given aspects
   *
   * @param urn the id of the entity
   * @param aspectNames aspects of the entity
   * @return response object
   * @throws RemoteInvocationException
   * @throws URISyntaxException
   */
  @Nullable
  default EntityResponse getV2(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nullable Set<String> aspectNames)
      throws RemoteInvocationException, URISyntaxException {
    getOperationContextMap().put(opContext.getEntityContextId(), opContext);
    return getEntityClientCache()
        .getV2(opContext, urn, aspectNames != null ? aspectNames : Set.of());
  }

  /**
   * Batch get a set of aspects for a single entity type, multiple ids with the given aspects.
   *
   * @param urns the urns of the entities to batch get
   * @param aspectNames the aspect names to batch get
   * @throws RemoteInvocationException
   */
  @Nonnull
  default Map<Urn, EntityResponse> batchGetV2(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> urns,
      @Nullable Set<String> aspectNames)
      throws RemoteInvocationException, URISyntaxException {
    getOperationContextMap().put(opContext.getEntityContextId(), opContext);
    return getEntityClientCache()
        .batchGetV2(opContext, urns, aspectNames != null ? aspectNames : Set.of());
  }

  Map<Urn, EntityResponse> batchGetV2NoCache(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull Set<Urn> urns,
      @Nullable Set<String> aspectNames)
      throws RemoteInvocationException, URISyntaxException;
}
