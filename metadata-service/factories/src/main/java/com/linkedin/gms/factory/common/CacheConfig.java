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
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
  public CacheManager hazelcastCacheManager() {
    Config config = new Config();
    // TODO: This setting is equivalent to expireAfterAccess, refreshes timer after a get, put,
    // containsKey etc.
    //       is this behavior what we actually desire? Should we change it now?
    MapConfig mapConfig = new MapConfig().setMaxIdleSeconds(cacheTtlSeconds);

    EvictionConfig evictionConfig =
        new EvictionConfig()
            .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
            .setSize(cacheMaxSize)
            .setEvictionPolicy(EvictionPolicy.LFU);
    mapConfig.setEvictionConfig(evictionConfig);
    mapConfig.setName("default");
    config.addMapConfig(mapConfig);

    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config
        .getNetworkConfig()
        .getJoin()
        .getKubernetesConfig()
        .setEnabled(true)
        .setProperty("service-dns", hazelcastServiceName);

    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

    return new HazelcastCacheManager(hazelcastInstance);
  }
}
