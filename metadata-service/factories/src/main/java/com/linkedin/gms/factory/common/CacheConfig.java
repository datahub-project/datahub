package com.linkedin.gms.factory.common;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.spring.cache.HazelcastCacheManager;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.GraphDefinition;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.NearCache;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.config.hazelcast.HazelcastInstanceBootstrapCondition;
import com.linkedin.metadata.config.hazelcast.RateLimitEndpointEnabledCondition;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotSerializer;
import com.linkedin.metadata.graph.cache.store.EntityGraphOperationalStatus;
import com.linkedin.metadata.graph.cache.store.EntityGraphOperationalStatusSerializer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CacheConfig {
  public static final String THROTTLE_MAP = "distributedThrottle";

  @Value("${cache.primary.ttlSeconds:600}")
  private int cacheTtlSeconds;

  @Value("${cache.primary.maxSize:10000}")
  private int cacheMaxSize;

  @Value("${searchService.cache.hazelcast.serviceName:hazelcast-service}")
  private String hazelcastServiceName;

  @Value("${searchService.cache.hazelcast.kubernetes-api-retries:}")
  private String kubernetesApiRetries;

  @Value("${searchService.cache.hazelcast.service-dns-timeout:}")
  private String kubernetesServiceDnsTimeout;

  @Value("${searchService.cache.hazelcast.resolve-not-ready-addresses:}")
  private String kubernetesResolveNotReadyAddresses;

  @Value("${datahub.gms.rateLimits.endpoint.hazelcastMapName:gmsRateLimitEndpointBuckets}")
  private String rateLimitEndpointHazelcastMapName;

  @Value("${datahub.gms.rateLimits.endpoint.bucketMaxIdleSeconds:300}")
  private int rateLimitEndpointBucketMaxIdleSeconds;

  @Value("${datahub.gms.rateLimits.endpoint.bucketMaxSize:100000}")
  private int rateLimitEndpointBucketMaxSize;

  @Bean
  @ConditionalOnProperty(name = "searchService.cacheImplementation", havingValue = "caffeine")
  public CacheManager caffeineCacheManager() {
    CaffeineCacheManager cacheManager = new CaffeineCacheManager();
    cacheManager.setCaffeine(caffeineCacheBuilder());
    return cacheManager;
  }

  private Caffeine<Object, Object> caffeineCacheBuilder() {
    return Caffeine.newBuilder()
        .initialCapacity(100)
        .maximumSize(cacheMaxSize)
        .expireAfterWrite(cacheTtlSeconds, TimeUnit.SECONDS)
        .recordStats();
  }

  @Bean("hazelcastInstance")
  @Conditional(HazelcastInstanceBootstrapCondition.class)
  public HazelcastInstance hazelcastInstance(
      List<MapConfig> hazelcastMapConfigs,
      List<ReplicatedMapConfig> hazelcastReplicatedMapConfigs,
      ObjectProvider<EntityGraphCacheProperties> effectiveEntityGraphCachePropertiesProvider) {
    Config config = new Config();
    EntityGraphCacheProperties effectiveEntityGraphCacheProperties =
        effectiveEntityGraphCachePropertiesProvider.getIfAvailable();

    hazelcastMapConfigs.forEach(config::addMapConfig);
    hazelcastReplicatedMapConfigs.forEach(config::addReplicatedMapConfig);

    if (effectiveEntityGraphCacheProperties != null
        && effectiveEntityGraphCacheProperties.isEnabled()) {
      boolean hasFullGraph =
          effectiveEntityGraphCacheProperties.getGraphs().values().stream()
              .anyMatch(
                  graph ->
                      graph != null
                          && graph.isEnabled()
                          && (graph.getScope() == null
                              || graph.getScope().getMode() != ScopeMode.PARTIAL));
      if (hasFullGraph) {
        config.addMapConfig(
            buildEntityGraphFullSnapshotMapConfig(effectiveEntityGraphCacheProperties));
      }
      effectiveEntityGraphCacheProperties
          .getGraphs()
          .forEach(
              (graphId, graph) -> {
                if (graph != null
                    && graph.isEnabled()
                    && graph.getScope() != null
                    && graph.getScope().getMode() == ScopeMode.PARTIAL) {
                  config.addMapConfig(
                      buildEntityGraphPartialSnapshotMapConfig(
                          graphId, graph, effectiveEntityGraphCacheProperties));
                }
              });
    }

    config
        .getSerializationConfig()
        .addSerializerConfig(
            new SerializerConfig()
                .setTypeClass(EntityGraphSnapshot.class)
                .setImplementation(new EntityGraphSnapshotSerializer()))
        .addSerializerConfig(
            new SerializerConfig()
                .setTypeClass(EntityGraphOperationalStatus.class)
                .setImplementation(new EntityGraphOperationalStatusSerializer()));

    config.setClassLoader(this.getClass().getClassLoader());
    config.setProperty("hazelcast.logging.type", "slf4j");

    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

    var kubernetesConfig =
        config.getNetworkConfig().getJoin().getKubernetesConfig().setEnabled(true);

    kubernetesConfig.setProperty("service-dns", hazelcastServiceName);

    if (!kubernetesApiRetries.isEmpty()) {
      kubernetesConfig.setProperty("kubernetes-api-retries", kubernetesApiRetries);
    }

    if (!kubernetesServiceDnsTimeout.isEmpty()) {
      kubernetesConfig.setProperty("service-dns-timeout", kubernetesServiceDnsTimeout);
    }

    if (!kubernetesResolveNotReadyAddresses.isEmpty()) {
      kubernetesConfig.setProperty(
          "resolve-not-ready-addresses", kubernetesResolveNotReadyAddresses);
    }

    return Hazelcast.newHazelcastInstance(config);
  }

  @Bean
  @ConditionalOnProperty(name = "searchService.cacheImplementation", havingValue = "hazelcast")
  public CacheManager hazelcastCacheManager(
      @Qualifier("hazelcastInstance") final HazelcastInstance hazelcastInstance) {
    return new HazelcastCacheManager(hazelcastInstance);
  }

  /** Search cache map defaults — only when search uses Hazelcast as {@link CacheManager}. */
  @Bean
  @ConditionalOnProperty(name = "searchService.cacheImplementation", havingValue = "hazelcast")
  public MapConfig defaultMapConfig() {
    MapConfig mapConfig = new MapConfig().setTimeToLiveSeconds(cacheTtlSeconds);

    EvictionConfig evictionConfig =
        new EvictionConfig()
            .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
            .setSize(cacheMaxSize)
            .setEvictionPolicy(EvictionPolicy.LFU);
    mapConfig.setEvictionConfig(evictionConfig);
    mapConfig.setName("default");
    return mapConfig;
  }

  @Bean
  @Conditional(HazelcastInstanceBootstrapCondition.class)
  public ReplicatedMapConfig distributedThrottleMapConfig() {
    ReplicatedMapConfig mapConfig = new ReplicatedMapConfig();
    mapConfig
        .setName(THROTTLE_MAP)
        .setInMemoryFormat(InMemoryFormat.OBJECT)
        .setMergePolicyConfig(
            new MergePolicyConfig().setPolicy(LatestUpdateMergePolicy.class.getName()));

    return mapConfig;
  }

  @Bean
  @Conditional(RateLimitEndpointEnabledCondition.class)
  public MapConfig rateLimitEndpointBucketsMapConfig() {
    MapConfig mapConfig = new MapConfig();
    mapConfig.setName(rateLimitEndpointHazelcastMapName).setBackupCount(1).setAsyncBackupCount(0);

    // Idle eviction (not TTL): an idle actor's entry expires and its bucket re-initialises to full
    // on the next consume, which is the correct behaviour for a token-bucket limiter.  TTL would
    // reset buckets for actors that are actively sending requests.
    mapConfig.setMaxIdleSeconds(rateLimitEndpointBucketMaxIdleSeconds);

    // LRU eviction caps per-node memory when the actor population grows very large.  The limit is
    // tuned via config; idle eviction above handles the common case of actors that stop sending
    // requests.
    EvictionConfig evictionConfig =
        new EvictionConfig()
            .setEvictionPolicy(EvictionPolicy.LRU)
            .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
            .setSize(rateLimitEndpointBucketMaxSize);
    mapConfig.setEvictionConfig(evictionConfig);

    return mapConfig;
  }

  @Bean
  @Conditional(RateLimitEndpointEnabledCondition.class)
  public MapConfig rateLimitGlobalBucketsMapConfig() {
    // The fleet-wide ceiling lives under a single "global" key, so this map needs no eviction (it
    // would only ever hold one entry, and evicting it would reset the cross-tenant ceiling under
    // active load). Registered explicitly for an owner backup so the ceiling survives a node loss.
    return new MapConfig()
        .setName(RateLimitProperties.Endpoint.GLOBAL_HAZELCAST_MAP_NAME)
        .setBackupCount(1)
        .setAsyncBackupCount(0);
  }

  @Bean
  @ConditionalOnProperty(name = "datahub.gms.entityGraphCache.enabled", havingValue = "true")
  @ConditionalOnBean(name = "effectiveEntityGraphCacheProperties")
  public MapConfig entityGraphStatusMapConfig(
      EntityGraphCacheProperties effectiveEntityGraphCacheProperties) {
    // Operational status must not be size-evicted: evicted BUILDING/COOLDOWN entries can make
    // getStatus() fall back to stale ACTIVE snapshot status.
    return new MapConfig()
        .setName(EntityGraphCacheProperties.STATUS_MAP)
        .setBackupCount(
            effectiveEntityGraphCacheProperties.getEviction().getHazelcast().getBackupCount());
  }

  private static MapConfig buildEntityGraphFullSnapshotMapConfig(
      @Nonnull EntityGraphCacheProperties properties) {
    return buildEntityGraphSnapshotMapConfig(
        EntityGraphCacheProperties.FULL_SNAPSHOTS_MAP,
        resolveScopeNearCache(properties).getFull(),
        properties);
  }

  private static MapConfig buildEntityGraphPartialSnapshotMapConfig(
      @Nonnull String graphId,
      @Nonnull GraphDefinition graph,
      @Nonnull EntityGraphCacheProperties properties) {
    return buildEntityGraphSnapshotMapConfig(
        EntityGraphCacheProperties.partialSnapshotsMapName(graphId),
        resolvePartialNearCache(graph, properties),
        properties);
  }

  @Nonnull
  private static NearCache resolvePartialNearCache(
      @Nonnull GraphDefinition graph, @Nonnull EntityGraphCacheProperties properties) {
    if (graph.getEviction() != null && graph.getEviction().getNearCache() != null) {
      return graph.getEviction().getNearCache();
    }
    return resolveScopeNearCache(properties).getPartial();
  }

  private static MapConfig buildEntityGraphSnapshotMapConfig(
      @Nonnull String mapName,
      @Nonnull NearCache nearCache,
      @Nonnull EntityGraphCacheProperties properties) {
    EntityGraphCacheProperties.HazelcastEviction hazelcastEviction =
        properties.getEviction().getHazelcast();

    MapConfig mapConfig =
        new MapConfig()
            .setName(mapName)
            .setBackupCount(hazelcastEviction.getBackupCount())
            .setInMemoryFormat(InMemoryFormat.BINARY)
            .setEvictionConfig(buildEntityGraphEvictionConfig(hazelcastEviction));

    if (hazelcastEviction.getTtlSeconds() > 0) {
      mapConfig.setTimeToLiveSeconds(hazelcastEviction.getTtlSeconds());
    }

    if (nearCache.isEnabled()) {
      mapConfig.setNearCacheConfig(buildNearCacheConfig(nearCache));
    }

    return mapConfig;
  }

  @Nonnull
  private static EntityGraphCacheProperties.ScopeNearCache resolveScopeNearCache(
      @Nonnull EntityGraphCacheProperties properties) {
    EntityGraphCacheProperties.Eviction eviction = properties.getEviction();
    if (eviction == null || eviction.getNearCache() == null) {
      throw new IllegalStateException(
          "entityGraphCache.eviction.nearCache is required — configure defaults in application.yaml");
    }
    return eviction.getNearCache();
  }

  @Nonnull
  private static NearCacheConfig buildNearCacheConfig(@Nonnull NearCache nearCache) {
    return new NearCacheConfig()
        .setInMemoryFormat(InMemoryFormat.BINARY)
        .setInvalidateOnChange(true)
        .setEvictionConfig(
            new EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LFU)
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT)
                .setSize(nearCache.getMaxSize()));
  }

  private static EvictionConfig buildEntityGraphEvictionConfig(
      EntityGraphCacheProperties.HazelcastEviction hazelcastEviction) {
    EvictionConfig evictionConfig = new EvictionConfig();
    String evictionPolicy = hazelcastEviction.getEvictionPolicy();

    if (InternalEvictionPolicy.MAX_SIZE.isPolicy(evictionPolicy)
        || InternalEvictionPolicy.SIZE_AND_TTL.isPolicy(evictionPolicy)) {
      evictionConfig
          .setEvictionPolicy(EvictionPolicy.LFU)
          .setMaxSizePolicy(resolveMaxSizePolicy(hazelcastEviction.getMaxSizePolicy()))
          .setSize(hazelcastEviction.getMaxSizePerNode());
    } else if (InternalEvictionPolicy.NEVER.isPolicy(evictionPolicy)) {
      evictionConfig.setEvictionPolicy(EvictionPolicy.NONE).setSize(0);
    } else {
      evictionConfig
          .setEvictionPolicy(EvictionPolicy.LFU)
          .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
          .setSize(hazelcastEviction.getMaxSizePerNode());
    }

    if (hazelcastEviction.getHeapMaxSizePercent() > 0) {
      evictionConfig
          .setMaxSizePolicy(MaxSizePolicy.USED_HEAP_PERCENTAGE)
          .setSize(hazelcastEviction.getHeapMaxSizePercent());
    }

    return evictionConfig;
  }

  private static MaxSizePolicy resolveMaxSizePolicy(String policyName) {
    if ("USED_HEAP_PERCENTAGE".equalsIgnoreCase(policyName)) {
      return MaxSizePolicy.USED_HEAP_PERCENTAGE;
    }
    if ("ENTRY_COUNT".equalsIgnoreCase(policyName)) {
      return MaxSizePolicy.ENTRY_COUNT;
    }
    return MaxSizePolicy.PER_NODE;
  }

  public static final String KEY_ASPECT_ENTITY_COUNTS_MAP = "keyAspectEntityCounts";
  public static final String KEY_ASPECT_ENTITY_COUNTS_IN_FLIGHT_MAP =
      "keyAspectEntityCountsInFlight";

  @Bean
  @Conditional(HazelcastInstanceBootstrapCondition.class)
  public MapConfig keyAspectEntityCountsMapConfig(
      @Value("${cache.entityCounts.keyAspect.ttlSeconds:3600}") int ttlSeconds,
      @Value("${cache.primary.maxSize:10000}") int cacheMaxSize) {
    MapConfig mapConfig = new MapConfig(KEY_ASPECT_ENTITY_COUNTS_MAP);
    mapConfig.setTimeToLiveSeconds(ttlSeconds);
    EvictionConfig evictionConfig =
        new EvictionConfig()
            .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
            .setSize(cacheMaxSize)
            .setEvictionPolicy(EvictionPolicy.LFU);
    mapConfig.setEvictionConfig(evictionConfig);
    return mapConfig;
  }

  @Bean
  @Conditional(HazelcastInstanceBootstrapCondition.class)
  public MapConfig keyAspectEntityCountsInFlightMapConfig() {
    return new MapConfig(KEY_ASPECT_ENTITY_COUNTS_IN_FLIGHT_MAP);
  }
}
