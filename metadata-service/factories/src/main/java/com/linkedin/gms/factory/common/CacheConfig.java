package com.linkedin.gms.factory.common;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spring.cache.HazelcastCacheManager;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.cache.CacheConfiguration;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import static com.linkedin.gms.factory.search.LineageSearchServiceFactory.*;


@Configuration
public class CacheConfig {

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
        .expireAfterAccess(cacheTtlSeconds, TimeUnit.SECONDS)
        .recordStats();
  }

  @Bean
  @ConditionalOnProperty(name = "searchService.cacheImplementation", havingValue = "hazelcast")
  @Primary
  public CacheManager hazelcastCacheManager(ConfigurationProvider configurationProvider) {
    Config config = new Config();

    // Set up default map configuration
    // TODO: This setting is equivalent to expireAfterAccess, refreshes timer after a get, put, containsKey etc.
    //       is this behavior what we actually desire? Should we change it now?
    MapConfig mapConfig = new MapConfig().setMaxIdleSeconds(cacheTtlSeconds);

    EvictionConfig evictionConfig = new EvictionConfig().setMaxSizePolicy(MaxSizePolicy.PER_NODE)
        .setSize(cacheMaxSize)
        .setEvictionPolicy(EvictionPolicy.LFU);
    mapConfig.setEvictionConfig(evictionConfig);
    mapConfig.setName("default");
    config.addMapConfig(mapConfig);

    // Set up special configuration for lineage cache
    config.addMapConfig(lineageCacheConfig(configurationProvider));

    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getKubernetesConfig().setEnabled(true)
        .setProperty("service-dns", hazelcastServiceName);

    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

    return new HazelcastCacheManager(hazelcastInstance);
  }

  private MapConfig lineageCacheConfig(ConfigurationProvider configurationProvider) {
    CacheConfiguration cacheConfiguration = configurationProvider.getCache();
    MapConfig lineageMapConfig = new MapConfig();
    String evictionPolicy = cacheConfiguration.getSearch().getLineage().getEvictionPolicy();
    if (InternalEvictionPolicy.TTL.isPolicy(evictionPolicy) || InternalEvictionPolicy.SIZE_AND_TTL.isPolicy(
        evictionPolicy)) {
      int ttl = Long.valueOf(cacheConfiguration.getSearch().getLineage().getTtlSeconds()).intValue();
      lineageMapConfig.setTimeToLiveSeconds(ttl);
    }

    EvictionConfig evictionConfig = new EvictionConfig();

    if (InternalEvictionPolicy.MAX_SIZE.isPolicy(evictionPolicy) || InternalEvictionPolicy.SIZE_AND_TTL.isPolicy(
        evictionPolicy)) {
      evictionConfig.setMaxSizePolicy(MaxSizePolicy.PER_NODE).setSize(cacheMaxSize).setEvictionPolicy(EvictionPolicy.LFU);
    } else {
      evictionConfig.setSize(0) // Ignored
          .setEvictionPolicy(EvictionPolicy.NONE); // Will never evict based on size, allows infinite growth of cache entries
    }
    lineageMapConfig.setEvictionConfig(evictionConfig);
    lineageMapConfig.setName(LINEAGE_SEARCH_SERVICE_CACHE_NAME);
    return lineageMapConfig;
  }
}