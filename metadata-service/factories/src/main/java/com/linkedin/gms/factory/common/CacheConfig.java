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
import com.linkedin.metadata.config.hazelcast.HazelcastInstanceBootstrapCondition;
import com.linkedin.metadata.config.hazelcast.RateLimitEndpointEnabledCondition;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
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
      List<ReplicatedMapConfig> hazelcastReplicatedMapConfigs) {
    Config config = new Config();

    hazelcastMapConfigs.forEach(config::addMapConfig);
    hazelcastReplicatedMapConfigs.forEach(config::addReplicatedMapConfig);

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
}
