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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

@Slf4j
final class EndpointRateLimitStore {
  private final Map<String, RegisteredEndpointBucket> buckets = new HashMap<>();
  private final ProxyManager<String> distributedProxyManager;

  EndpointRateLimitStore(
      @Nullable RateLimitProperties.Endpoint endpointConfig,
      @Nonnull HazelcastInstance hazelcastInstance) {
    String mapName = resolveMapName(endpointConfig);
    IMap<String, byte[]> map = hazelcastInstance.getMap(mapName);
    this.distributedProxyManager = Bucket4jHazelcast.entryProcessorBasedBuilder(map).build();
    log.info("Endpoint rate limits use distributed Hazelcast map {}", mapName);
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
    buckets.put(rule.getId(), new RegisteredEndpointBucket(bucket, capacity));
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

    private RegisteredEndpointBucket(Bucket bucket, int configuredCapacity) {
      this.bucket = bucket;
      this.configuredCapacity = configuredCapacity;
    }

    private Bucket getBucket() {
      return bucket;
    }

    private int getConfiguredCapacity() {
      return configuredCapacity;
    }
  }
}
