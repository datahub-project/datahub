package com.linkedin.gms.factory.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.config.GMSConfiguration;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheConfigLoader;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties;
import com.linkedin.metadata.config.hazelcast.HazelcastBootstrapProperties;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphRegistry;
import com.linkedin.metadata.graph.cache.service.EntityGraphCacheScheduler;
import com.linkedin.metadata.graph.cache.service.EntityGraphCacheService;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphLocalViewCache;
import com.linkedin.metadata.graph.cache.store.EntityGraphMemoryPressureMonitor;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * Entity graph cache wiring. Controlled by {@link
 * HazelcastBootstrapProperties#ENTITY_GRAPH_CACHE_ENABLED} ({@code
 * datahub.gms.entityGraphCache.enabled} / {@code ENTITY_GRAPH_CACHE_ENABLED}).
 *
 * <p>When enabled (default on GMS): loads config, registry, Hazelcast-backed {@link
 * EntityGraphCacheService}, scheduler, and memory monitor. When disabled (MAE/MCE/upgrade module
 * defaults): registers {@link EntityGraphCache#NO_OP} only.
 */
@Slf4j
@Configuration
public class EntityGraphCacheFactory {

  private static final int BUNDLED_DOMAIN_MAX_VERTICES_WARNING_THRESHOLD = 500;

  @Bean
  @ConditionalOnProperty(
      name = HazelcastBootstrapProperties.ENTITY_GRAPH_CACHE_ENABLED,
      havingValue = "true")
  @Nonnull
  public EntityGraphCacheConfigLoader entityGraphCacheConfigLoader(ObjectMapper objectMapper) {
    return new EntityGraphCacheConfigLoader(
        objectMapper, ObjectMapperContext.DEFAULT.getYamlMapper());
  }

  @Bean
  @ConditionalOnProperty(
      name = HazelcastBootstrapProperties.ENTITY_GRAPH_CACHE_ENABLED,
      havingValue = "true")
  @Nonnull
  public EntityGraphCacheProperties effectiveEntityGraphCacheProperties(
      GMSConfiguration gmsConfiguration, EntityGraphCacheConfigLoader configLoader) {
    EntityGraphCacheProperties fromSpring =
        gmsConfiguration.getEntityGraphCache() != null
            ? gmsConfiguration.getEntityGraphCache()
            : new EntityGraphCacheProperties();
    EntityGraphCacheProperties effective = configLoader.loadEffective(fromSpring);
    return effective;
  }

  @Bean
  @ConditionalOnProperty(
      name = HazelcastBootstrapProperties.ENTITY_GRAPH_CACHE_ENABLED,
      havingValue = "true")
  @Nonnull
  public EntityGraphRegistry entityGraphRegistry(
      EntityGraphCacheProperties effectiveEntityGraphCacheProperties,
      EntityRegistry entityRegistry) {
    LineageRegistry lineageRegistry = new LineageRegistry(entityRegistry);
    EntityGraphRegistry registry =
        EntityGraphRegistry.build(
            effectiveEntityGraphCacheProperties, entityRegistry, lineageRegistry);
    logBundledDomainVertexCapWarning(registry);
    return registry;
  }

  private static void logBundledDomainVertexCapWarning(@Nonnull EntityGraphRegistry registry) {
    EntityGraphDefinition domain = registry.getDefinition(KnownEntityGraph.DOMAIN.getConfigKey());
    if (domain == null
        || !domain.isEnabled()
        || domain.getBounds() == null
        || domain.getBounds().getMaxVertices() > BUNDLED_DOMAIN_MAX_VERTICES_WARNING_THRESHOLD) {
      return;
    }
    int maxEdges =
        domain.getBounds().getMaxEdges().isPresent()
            ? domain.getBounds().getMaxEdges().getAsInt()
            : -1;
    log.warn(
        "Bundled domain entity graph cache maxVertices is {} (maxEdges {}). "
            + "Deployments with more domains in the search index hit OVER_LIMIT at build time; "
            + "the cache is bypassed until bounds are raised via ENTITY_GRAPH_CACHE_CONFIG_JSON. "
            + "See docs/deploy/gms-entity-graph-cache.md.",
        domain.getBounds().getMaxVertices(),
        maxEdges);
  }

  @Bean(destroyMethod = "shutdown")
  @ConditionalOnProperty(
      name = HazelcastBootstrapProperties.ENTITY_GRAPH_CACHE_ENABLED,
      havingValue = "true")
  @Nonnull
  public ExecutorService entityGraphRebuildExecutor() {
    return Executors.newSingleThreadExecutor(
        r -> {
          Thread t = new Thread(r, "entity-graph-rebuild");
          t.setDaemon(true);
          return t;
        });
  }

  /**
   * Internal implementation — inject by name only ({@code @Qualifier("entityGraphCacheService")}).
   * Call sites use {@link #entityGraphCache} ({@code @Qualifier("entityGraphCache")}).
   */
  @Bean(name = "entityGraphCacheService")
  @ConditionalOnProperty(
      name = HazelcastBootstrapProperties.ENTITY_GRAPH_CACHE_ENABLED,
      havingValue = "true")
  @Nonnull
  public EntityGraphCacheService entityGraphCacheService(
      EntityGraphCacheProperties effectiveEntityGraphCacheProperties,
      EntityGraphRegistry entityGraphRegistry,
      @Autowired(required = false) @Qualifier("hazelcastInstance")
          HazelcastInstance hazelcastInstance,
      @Qualifier("systemOperationContext") @Lazy OperationContext systemOperationContext,
      @Qualifier("entityGraphRebuildExecutor") ExecutorService entityGraphRebuildExecutor) {

    HazelcastInstance hazelcast =
        Objects.requireNonNull(
            hazelcastInstance,
            "Entity graph cache requires Hazelcast — verify the cluster is reachable via "
                + "searchService.cache.hazelcast.serviceName (default hazelcast-service)");

    EntityGraphLocalViewCache localViews = new EntityGraphLocalViewCache();
    EntityGraphSnapshotBuilder snapshotBuilder = new EntityGraphSnapshotBuilder();
    AtomicReference<EntityGraphCacheService> serviceRef = new AtomicReference<>();

    EntityGraphDistributedStore distributedStore =
        new EntityGraphDistributedStore(
            hazelcast,
            entityGraphRegistry,
            cacheKey -> {
              EntityGraphCacheService svc = serviceRef.get();
              if (svc != null) {
                svc.onSnapshotUpdated(cacheKey);
              }
            });

    EntityGraphCacheService service =
        new EntityGraphCacheService(
            effectiveEntityGraphCacheProperties,
            entityGraphRegistry,
            distributedStore,
            localViews,
            snapshotBuilder,
            systemOperationContext,
            entityGraphRebuildExecutor);
    serviceRef.set(service);
    return service;
  }

  /**
   * Public {@link EntityGraphCache} API — real service when enabled, {@link EntityGraphCache#NO_OP}
   * otherwise.
   */
  @Bean(name = "entityGraphCache")
  @Nonnull
  public EntityGraphCache entityGraphCache(
      ObjectProvider<EntityGraphCacheService> entityGraphCacheServiceProvider) {
    EntityGraphCacheService service = entityGraphCacheServiceProvider.getIfAvailable();
    return service != null ? service : EntityGraphCache.NO_OP;
  }

  @Bean
  @ConditionalOnProperty(
      name = HazelcastBootstrapProperties.ENTITY_GRAPH_CACHE_ENABLED,
      havingValue = "true")
  @Nonnull
  public EntityGraphMemoryPressureMonitor entityGraphMemoryPressureMonitor(
      EntityGraphCacheProperties effectiveEntityGraphCacheProperties,
      EntityGraphRegistry entityGraphRegistry,
      @Qualifier("entityGraphCacheService") EntityGraphCacheService entityGraphCacheService) {
    return new EntityGraphMemoryPressureMonitor(
        effectiveEntityGraphCacheProperties, entityGraphCacheService, entityGraphRegistry);
  }

  @Bean
  @ConditionalOnProperty(
      name = HazelcastBootstrapProperties.ENTITY_GRAPH_CACHE_ENABLED,
      havingValue = "true")
  @Nonnull
  public EntityGraphCacheScheduler entityGraphCacheScheduler(
      EntityGraphRegistry entityGraphRegistry,
      @Qualifier("entityGraphCacheService") EntityGraphCacheService entityGraphCacheService) {
    return new EntityGraphCacheScheduler(entityGraphRegistry, entityGraphCacheService);
  }
}
