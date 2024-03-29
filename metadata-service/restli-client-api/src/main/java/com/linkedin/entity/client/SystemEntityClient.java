package com.linkedin.entity.client;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Adds entity/aspect cache and assumes **system** authentication */
public interface SystemEntityClient extends EntityClient {

  EntityClientCache getEntityClientCache();

  @Nonnull
  ConcurrentHashMap<String, OperationContext> getOperationContextMap();

  @Nonnull
  OperationContext getSystemOperationContext();

  default Authentication getSystemAuthentication() {
    return getSystemOperationContext().getAuthentication();
  }

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
                String entityName =
                    collectionKey.getUrns().stream().findFirst().map(Urn::getEntityType).get();

                if (collectionKey.getUrns().stream()
                    .anyMatch(urn -> !urn.getEntityType().equals(entityName))) {
                  throw new IllegalArgumentException(
                      "Urns must be of the same entity type. RestliEntityClient API limitation.");
                }

                return batchGetV2(
                    entityName,
                    collectionKey.getUrns(),
                    collectionKey.getAspectNames(),
                    getSystemOperationContext().getAuthentication());
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
  default EntityResponse getV2(@Nonnull Urn urn, @Nonnull Set<String> aspectNames)
      throws RemoteInvocationException, URISyntaxException {
    return getEntityClientCache().getV2(getSystemOperationContext(), urn, aspectNames);
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
      @Nonnull Set<Urn> urns, @Nonnull Set<String> aspectNames)
      throws RemoteInvocationException, URISyntaxException {
    return getEntityClientCache().batchGetV2(getSystemOperationContext(), urns, aspectNames);
  }

  default void producePlatformEvent(
      @Nonnull String name, @Nullable String key, @Nonnull PlatformEvent event) throws Exception {
    producePlatformEvent(name, key, event, getSystemAuthentication());
  }

  default boolean exists(@Nonnull Urn urn) throws RemoteInvocationException {
    return exists(urn, getSystemAuthentication());
  }

  default String ingestProposal(
      @Nonnull final MetadataChangeProposal metadataChangeProposal, final boolean async)
      throws RemoteInvocationException {
    return ingestProposal(metadataChangeProposal, getSystemAuthentication(), async);
  }

  default void setWritable(boolean canWrite) throws RemoteInvocationException {
    setWritable(canWrite, getSystemAuthentication());
  }

  @Nullable
  default Aspect getLatestAspectObject(@Nonnull Urn urn, @Nonnull String aspectName)
      throws RemoteInvocationException, URISyntaxException {
    return getLatestAspectObject(urn, aspectName, getSystemAuthentication());
  }

  @Nonnull
  default Map<Urn, Map<String, Aspect>> getLatestAspects(
      @Nonnull Set<Urn> urns, @Nonnull Set<String> aspectNames)
      throws RemoteInvocationException, URISyntaxException {
    return getLatestAspects(urns, aspectNames, getSystemAuthentication());
  }
}
