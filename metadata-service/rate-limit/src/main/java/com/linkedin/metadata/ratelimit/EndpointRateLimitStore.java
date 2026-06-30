package com.linkedin.metadata.ratelimit;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.ConsumptionProbe;
import io.github.bucket4j.Refill;
import io.github.bucket4j.distributed.proxy.ProxyManager;
import io.github.bucket4j.grid.hazelcast.Bucket4jHazelcast;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

@Slf4j
final class EndpointRateLimitStore {
  private static final String ACTOR_KEY_INFIX = ":actor:";

  /**
   * Shared map for fleet-wide (cross-tenant) buckets — the same name for every tenant, so the
   * {@code global} scope is one counter across the whole fleet. Tenant-scoped buckets live in the
   * tenant map ({@link #distributedProxyManager}, named per-tenant via {@code hazelcastMapName}).
   */
  public static final String GLOBAL_MAP_NAME = "gmsRateLimitGlobalBuckets";

  private final Map<String, RegisteredEndpointBucket> buckets = new HashMap<>();
  private final ProxyManager<String> distributedProxyManager;
  private final ProxyManager<String> globalProxyManager;

  /**
   * Caches one immutable {@link BucketConfiguration} per distinct limits value so the scoped hot
   * path doesn't rebuild Bandwidth/config objects on every request. Keyed by the limits POJO (value
   * equality via Lombok {@code @Data}); config is loaded once and not mutated at runtime.
   */
  private final Map<RateLimitProperties.BucketLimits, BucketConfiguration> scopedConfigCache =
      new ConcurrentHashMap<>();

  EndpointRateLimitStore(
      @Nullable RateLimitProperties.Endpoint endpointConfig,
      @Nonnull HazelcastInstance hazelcastInstance) {
    String mapName = resolveMapName(endpointConfig);
    IMap<String, byte[]> map = hazelcastInstance.getMap(mapName);
    this.distributedProxyManager = Bucket4jHazelcast.entryProcessorBasedBuilder(map).build();
    IMap<String, byte[]> globalMap = hazelcastInstance.getMap(GLOBAL_MAP_NAME);
    this.globalProxyManager = Bucket4jHazelcast.entryProcessorBasedBuilder(globalMap).build();
    log.info(
        "Endpoint rate limits use tenant map {} and shared global map {}",
        mapName,
        GLOBAL_MAP_NAME);
  }

  void registerEndpointRule(@Nonnull RateLimitProperties.Rule rule) {
    if (rule.getCapacity() == null
        || rule.getRefillTokens() == null
        || rule.getRefillPeriodSeconds() == null) {
      throw new IllegalStateException(
          "Endpoint rate limit rule " + rule.getId() + " requires capacity/refill fields");
    }

    int capacity = rule.getCapacity();
    int refillTokens = rule.getRefillTokens();
    int refillPeriodSeconds = rule.getRefillPeriodSeconds();

    BucketConfiguration configuration =
        bucketConfiguration(capacity, refillTokens, refillPeriodSeconds);
    Bucket bucket = distributedProxyManager.builder().build(rule.getId(), () -> configuration);
    buckets.put(rule.getId(), new RegisteredEndpointBucket(bucket, capacity, configuration));
    log.info(
        "Registered endpoint rate limit rule {} (cluster-wide capacity={})",
        rule.getId(),
        capacity);
  }

  @Nullable
  ConsumptionProbe tryConsumeAndReturnRemaining(@Nonnull String ruleId) {
    RegisteredEndpointBucket bucket = buckets.get(ruleId);
    if (bucket == null) {
      return null;
    }
    return bucket.getBucket().tryConsumeAndReturnRemaining(1);
  }

  @Nullable
  ConsumptionProbe tryConsumeForActor(@Nonnull String ruleId, @Nonnull String actorUrn) {
    RegisteredEndpointBucket registered = buckets.get(ruleId);
    if (registered == null) {
      return null;
    }
    BucketConfiguration config = registered.getConfiguration();
    Bucket bucket =
        distributedProxyManager.builder().build(ruleId + ACTOR_KEY_INFIX + actorUrn, () -> config);
    return bucket.tryConsumeAndReturnRemaining(1);
  }

  /**
   * Consumes {@code tokens} from the scoped bucket identified by {@code key}, creating it lazily
   * with the given limits. {@code globalMap=true} routes to the shared cross-tenant map (the {@code
   * global} scope); otherwise the per-tenant map. Returns the probe (never null — the bucket is
   * always materialized).
   */
  @Nonnull
  ConsumptionProbe tryConsumeScoped(
      @Nonnull String key,
      @Nonnull RateLimitProperties.BucketLimits limits,
      long tokens,
      boolean globalMap) {
    return scopedBucket(key, limits, globalMap).tryConsumeAndReturnRemaining(Math.max(1, tokens));
  }

  /**
   * Refunds {@code tokens} to the scoped bucket {@code key} (Bucket4j caps the add at capacity).
   * Used to roll back an already-consumed bucket when a later stage of the chain denies the
   * request, so a rejected request doesn't permanently burn upstream tenant/actor tokens.
   */
  void refundScoped(
      @Nonnull String key,
      @Nonnull RateLimitProperties.BucketLimits limits,
      long tokens,
      boolean globalMap) {
    scopedBucket(key, limits, globalMap).addTokens(Math.max(1, tokens));
  }

  @Nonnull
  private Bucket scopedBucket(
      @Nonnull String key, @Nonnull RateLimitProperties.BucketLimits limits, boolean globalMap) {
    ProxyManager<String> proxyManager = globalMap ? globalProxyManager : distributedProxyManager;
    BucketConfiguration configuration =
        scopedConfigCache.computeIfAbsent(
            limits,
            l ->
                bucketConfiguration(
                    l.getCapacity(), l.getRefillTokens(), l.getRefillPeriodSeconds()));
    return proxyManager.builder().build(key, () -> configuration);
  }

  double remaining(@Nonnull String ruleId) {
    RegisteredEndpointBucket bucket = buckets.get(ruleId);
    return bucket == null ? -1 : bucket.getBucket().getAvailableTokens();
  }

  int capacity(@Nonnull String ruleId) {
    RegisteredEndpointBucket bucket = buckets.get(ruleId);
    return bucket == null ? -1 : bucket.getConfiguredCapacity();
  }

  @Nonnull
  Map<String, Double> snapshotRemaining() {
    Map<String, Double> snapshot = new HashMap<>();
    buckets.forEach(
        (ruleId, bucket) -> snapshot.put(ruleId, (double) bucket.getBucket().getAvailableTokens()));
    return snapshot;
  }

  private static String resolveMapName(@Nullable RateLimitProperties.Endpoint endpointConfig) {
    if (endpointConfig != null && StringUtils.hasText(endpointConfig.getHazelcastMapName())) {
      return endpointConfig.getHazelcastMapName();
    }
    return RateLimitProperties.Endpoint.DEFAULT_HAZELCAST_MAP_NAME;
  }

  @Nonnull
  private static BucketConfiguration bucketConfiguration(
      int capacity, int refillTokens, int refillPeriodSeconds) {
    Bandwidth bandwidth =
        Bandwidth.classic(
            capacity,
            Refill.intervally(refillTokens, Duration.ofSeconds(Math.max(1, refillPeriodSeconds))));
    return BucketConfiguration.builder().addLimit(bandwidth).buildConfiguration();
  }

  private static final class RegisteredEndpointBucket {
    private final Bucket bucket;
    private final int configuredCapacity;
    private final BucketConfiguration configuration;

    private RegisteredEndpointBucket(
        Bucket bucket, int configuredCapacity, BucketConfiguration configuration) {
      this.bucket = bucket;
      this.configuredCapacity = configuredCapacity;
      this.configuration = configuration;
    }

    private Bucket getBucket() {
      return bucket;
    }

    private int getConfiguredCapacity() {
      return configuredCapacity;
    }

    private BucketConfiguration getConfiguration() {
      return configuration;
    }
  }
}
