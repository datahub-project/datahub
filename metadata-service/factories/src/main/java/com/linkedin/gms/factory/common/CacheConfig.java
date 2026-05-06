package com.linkedin.gms.factory.common;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.spring.cache.HazelcastCacheManager;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.TimeseriesAspectServiceConfig;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CacheConfig {
  public static final String THROTTLE_MAP = "distributedThrottle";

  @Value("${cache.primary.ttlSeconds:600}")
  private int cacheTtlSeconds;

  @Value("${cache.primary.maxSize:10000}")
  private int cacheMaxSize;

  // Single source of truth for the timeseries cache settings. The typed config object
  // ({@link TimeseriesAspectServiceConfig}) already binds TIMESERIES_CACHE_MAX_SIZE,
  // TIMESERIES_CACHE_TTL_HOURS, and friends — read from here rather than re-injecting via
  // @Value so the cache backend wiring and the rest of the system can never disagree about
  // the intended values.
  @Autowired private ConfigurationProvider configurationProvider;

  @Value("${searchService.cache.hazelcast.serviceName:hazelcast-service}")
  private String hazelcastServiceName;

  @Value("${searchService.cache.hazelcast.kubernetes-api-retries:}")
  private String kubernetesApiRetries;

  @Value("${searchService.cache.hazelcast.service-dns-timeout:}")
  private String kubernetesServiceDnsTimeout;

  @Value("${searchService.cache.hazelcast.resolve-not-ready-addresses:}")
  private String kubernetesResolveNotReadyAddresses;

  @Bean
  @ConditionalOnProperty(name = "searchService.cacheImplementation", havingValue = "caffeine")
  public CacheManager caffeineCacheManager() {
    CaffeineCacheManager cacheManager = new CaffeineCacheManager();
    cacheManager.setCaffeine(caffeineCacheBuilder());
    // The latest-timeseries cache has its own size budget (TIMESERIES_CACHE_MAX_SIZE); register
    // it as a custom cache so it doesn't inherit the smaller cache.primary.maxSize default
    // shared by every other Caffeine-backed cache in this manager. Two separate caches —
    // one for CachedLatestAspect data values, one for the Set<String> reverse index — keep
    // value types disjoint so the caching service can hold strongly-typed handles instead of
    // Object-erased ones.
    if (configurationProvider.getTimeseriesAspectService().getCache().isEnabled()) {
      cacheManager.registerCustomCache(
          "latestTimeseriesAspect", latestTimeseriesCaffeineBuilder().build());
      cacheManager.registerCustomCache(
          "latestTimeseriesAspectIndex", latestTimeseriesCaffeineBuilder().build());
    }
    return cacheManager;
  }

  private Caffeine<Object, Object> caffeineCacheBuilder() {
    return Caffeine.newBuilder()
        .initialCapacity(100)
        .maximumSize(cacheMaxSize)
        .expireAfterWrite(cacheTtlSeconds, TimeUnit.SECONDS)
        .recordStats();
  }

  private Caffeine<Object, Object> latestTimeseriesCaffeineBuilder() {
    TimeseriesAspectServiceConfig.CacheConfig tsCache =
        configurationProvider.getTimeseriesAspectService().getCache();
    return Caffeine.newBuilder()
        .initialCapacity(100)
        .maximumSize(tsCache.getMaxSize())
        .expireAfterWrite(tsCache.getTtlHours(), TimeUnit.HOURS)
        .recordStats();
  }

  @Bean("hazelcastInstance")
  @ConditionalOnProperty(name = "searchService.cacheImplementation", havingValue = "hazelcast")
  public HazelcastInstance hazelcastInstance(
      List<MapConfig> hazelcastMapConfigs,
      List<ReplicatedMapConfig> hazelcastReplicatedMapConfigs) {
    Config config = new Config();

    hazelcastMapConfigs.forEach(config::addMapConfig);
    hazelcastReplicatedMapConfigs.forEach(config::addReplicatedMapConfig);

    // Force classloader to load from application code
    config.setClassLoader(this.getClass().getClassLoader());

    // Route Hazelcast logs through SLF4J/logback instead of JUL
    config.setProperty("hazelcast.logging.type", "slf4j");

    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

    return Hazelcast.newHazelcastInstance(config);
  }

  @Bean
  @ConditionalOnProperty(name = "searchService.cacheImplementation", havingValue = "hazelcast")
  public CacheManager hazelcastCacheManager(
      @Qualifier("hazelcastInstance") final HazelcastInstance hazelcastInstance) {
    return new HazelcastCacheManager(hazelcastInstance);
  }

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
  @ConditionalOnProperty(name = "searchService.cacheImplementation", havingValue = "hazelcast")
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
  @ConditionalOnProperty(name = "searchService.cacheImplementation", havingValue = "hazelcast")
  public MapConfig latestTimeseriesCacheConfig() {
    int tsMaxSize = configurationProvider.getTimeseriesAspectService().getCache().getMaxSize();
    MapConfig mapConfig = new MapConfig().setName("latestTimeseriesAspect");
    EvictionConfig evictionConfig =
        new EvictionConfig()
            .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
            .setSize(tsMaxSize)
            .setEvictionPolicy(EvictionPolicy.LFU);
    mapConfig.setEvictionConfig(evictionConfig);
    return mapConfig;
  }

  /**
   * Companion map to {@link #latestTimeseriesCacheConfig()} that stores the (entity, aspect) →
   * Set&lt;URN&gt; reverse index used to evict all data keys for an aspect on
   * delete/reindex/rollback. Sized off the same TIMESERIES_CACHE_MAX_SIZE budget — there is one
   * index entry per (entity, aspect) pair, so this is a small fraction of the data map.
   */
  @Bean
  @ConditionalOnProperty(name = "searchService.cacheImplementation", havingValue = "hazelcast")
  public MapConfig latestTimeseriesAspectIndexCacheConfig() {
    int tsMaxSize = configurationProvider.getTimeseriesAspectService().getCache().getMaxSize();
    MapConfig mapConfig = new MapConfig().setName("latestTimeseriesAspectIndex");
    EvictionConfig evictionConfig =
        new EvictionConfig()
            .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
            .setSize(tsMaxSize)
            .setEvictionPolicy(EvictionPolicy.LFU);
    mapConfig.setEvictionConfig(evictionConfig);
    return mapConfig;
  }
}
