package com.linkedin.gms.factory.common;

import static com.linkedin.gms.factory.search.LineageSearchServiceFactory.*;

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
import com.linkedin.metadata.config.cache.CacheConfiguration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class CacheConfig {
  public static final String THROTTLE_MAP = "distributedThrottle";

  @Value("${cache.primary.ttlSeconds:600}")
  private int cacheTtlSeconds;

  @Value("${cache.primary.maxSize:10000}")
  private int cacheMaxSize;

  @Value("${searchService.cache.hazelcast.serviceName:hazelcast-service}")
  private String hazelcastServiceName;

  @Bean
  @ConditionalOnProperty(name = "searchService.cacheImplementation", havingValue = "caffeine")
  @Primary
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
  @ConditionalOnProperty(name = "searchService.cacheImplementation", havingValue = "hazelcast")
  public HazelcastInstance hazelcastInstance(
      List<MapConfig> hazelcastMapConfigs,
      List<ReplicatedMapConfig> hazelcastReplicatedMapConfigs) {
    Config config = new Config();

    hazelcastMapConfigs.forEach(config::addMapConfig);
    hazelcastReplicatedMapConfigs.forEach(config::addReplicatedMapConfig);

    // Force classloader to load from application code
    config.setClassLoader(this.getClass().getClassLoader());

    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config
        .getNetworkConfig()
        .getJoin()
        .getKubernetesConfig()
        .setEnabled(true)
        .setProperty("service-dns", hazelcastServiceName);

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
  public MapConfig lineageCacheConfig(final ConfigurationProvider configurationProvider) {
    CacheConfiguration cacheConfiguration = configurationProvider.getCache();
    MapConfig lineageMapConfig = new MapConfig();
    String evictionPolicy = cacheConfiguration.getSearch().getLineage().getEvictionPolicy();
    if (InternalEvictionPolicy.TTL.isPolicy(evictionPolicy)
        || InternalEvictionPolicy.SIZE_AND_TTL.isPolicy(evictionPolicy)) {
      int ttl =
          Long.valueOf(cacheConfiguration.getSearch().getLineage().getTtlSeconds()).intValue();
      lineageMapConfig.setTimeToLiveSeconds(ttl);
    }

    EvictionConfig evictionConfig = new EvictionConfig();

    if (InternalEvictionPolicy.MAX_SIZE.isPolicy(evictionPolicy)
        || InternalEvictionPolicy.SIZE_AND_TTL.isPolicy(evictionPolicy)) {
      evictionConfig
          .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
          .setSize(cacheMaxSize)
          .setEvictionPolicy(EvictionPolicy.LFU);
    } else {
      evictionConfig
          .setSize(0) // Ignored
          .setEvictionPolicy(
              EvictionPolicy
                  .NONE); // Will never evict based on size, allows infinite growth of cache entries
    }
    lineageMapConfig.setEvictionConfig(evictionConfig);
    lineageMapConfig.setName(LINEAGE_SEARCH_SERVICE_CACHE_NAME);
    return lineageMapConfig;
  }
}
