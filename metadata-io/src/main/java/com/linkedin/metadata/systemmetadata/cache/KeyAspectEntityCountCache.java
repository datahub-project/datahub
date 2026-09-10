package com.linkedin.metadata.systemmetadata.cache;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.linkedin.metadata.config.cache.KeyAspectEntityCountCacheConfiguration;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KeyAspectEntityCountCache {
  public static final String RESULT_MAP_NAME = "keyAspectEntityCounts";
  public static final String IN_FLIGHT_MAP_NAME = "keyAspectEntityCountsInFlight";

  private final KeyAspectEntityCountCacheConfiguration config;
  @Nullable private final IMap<KeyAspectEntityCountCacheKey, KeyAspectEntityCountResult> resultMap;

  @Nullable
  private final IMap<KeyAspectEntityCountCacheKey, KeyAspectEntityCountInFlightStatus> inFlightMap;

  private final Map<KeyAspectEntityCountCacheKey, KeyAspectEntityCountResult> localResultCache =
      new ConcurrentHashMap<>();
  private final Map<KeyAspectEntityCountCacheKey, Object> localLocks = new ConcurrentHashMap<>();

  public KeyAspectEntityCountCache(
      @Nonnull KeyAspectEntityCountCacheConfiguration config,
      @Nullable HazelcastInstance hazelcastInstance) {
    this.config = config;
    if (hazelcastInstance != null) {
      this.resultMap = hazelcastInstance.getMap(RESULT_MAP_NAME);
      this.inFlightMap = hazelcastInstance.getMap(IN_FLIGHT_MAP_NAME);
    } else {
      this.resultMap = null;
      this.inFlightMap = null;
    }
  }

  @Nonnull
  public KeyAspectEntityCountResult get(
      @Nonnull OperationContext opContext,
      @Nonnull KeyAspectEntityCountCacheKey cacheKey,
      boolean skipCache,
      @Nonnull CountLoader loader) {
    if (!config.isEnabled() || skipCache) {
      return loader.load().toBuilder().cacheHit(false).build();
    }

    KeyAspectEntityCountResult cached = getCachedResult(cacheKey);
    if (cached != null) {
      return cached.toBuilder().cacheHit(true).build();
    }

    if (inFlightMap != null) {
      return getWithDistributedSingleFlight(cacheKey, loader);
    }
    return getWithLocalSingleFlight(cacheKey, loader);
  }

  @Nonnull
  private KeyAspectEntityCountResult getWithDistributedSingleFlight(
      @Nonnull KeyAspectEntityCountCacheKey cacheKey, @Nonnull CountLoader loader) {
    long staleBuildingMillis = config.getSingleFlight().getStaleBuildingMillis();
    long waiterMaxMillis = config.getSingleFlight().getWaiterMaxMillis();
    long pollIntervalMillis = config.getSingleFlight().getPollIntervalMillis();
    long deadline = System.currentTimeMillis() + waiterMaxMillis;
    String claimantId = UUID.randomUUID().toString();

    while (System.currentTimeMillis() < deadline) {
      KeyAspectEntityCountResult cached = getCachedResult(cacheKey);
      if (cached != null) {
        return cached.toBuilder().cacheHit(true).build();
      }

      if (tryClaimQuery(cacheKey, claimantId, staleBuildingMillis)) {
        try {
          KeyAspectEntityCountResult loaded = loader.load();
          putCachedResult(cacheKey, loaded);
          return loaded.toBuilder().cacheHit(false).build();
        } finally {
          releaseInFlight(cacheKey);
        }
      }

      sleepQuietly(pollIntervalMillis);
    }

    log.warn(
        "Entity count single-flight waiter timed out for cache key {}; running local fallback query",
        cacheKey.getEntityTypesKey());
    return loader.load().toBuilder().cacheHit(false).build();
  }

  @Nonnull
  private KeyAspectEntityCountResult getWithLocalSingleFlight(
      @Nonnull KeyAspectEntityCountCacheKey cacheKey, @Nonnull CountLoader loader) {
    Object lock = localLocks.computeIfAbsent(cacheKey, ignored -> new Object());
    synchronized (lock) {
      KeyAspectEntityCountResult cached = getCachedResult(cacheKey);
      if (cached != null) {
        return cached.toBuilder().cacheHit(true).build();
      }
      KeyAspectEntityCountResult loaded = loader.load();
      putCachedResult(cacheKey, loaded);
      return loaded.toBuilder().cacheHit(false).build();
    }
  }

  @Nullable
  private KeyAspectEntityCountResult getCachedResult(
      @Nonnull KeyAspectEntityCountCacheKey cacheKey) {
    if (resultMap != null) {
      return resultMap.get(cacheKey);
    }
    KeyAspectEntityCountResult local = localResultCache.get(cacheKey);
    if (local == null || isExpired(local)) {
      localResultCache.remove(cacheKey);
      return null;
    }
    return local;
  }

  private void putCachedResult(
      @Nonnull KeyAspectEntityCountCacheKey cacheKey, @Nonnull KeyAspectEntityCountResult result) {
    if (resultMap != null) {
      resultMap.put(cacheKey, result);
    } else {
      localResultCache.put(cacheKey, result);
    }
  }

  private boolean isExpired(@Nonnull KeyAspectEntityCountResult result) {
    return result
        .getComputedAt()
        .isBefore(Instant.now().minus(config.getTtlSeconds(), ChronoUnit.SECONDS));
  }

  boolean tryClaimQuery(
      @Nonnull KeyAspectEntityCountCacheKey cacheKey,
      @Nonnull String claimantId,
      long staleBuildingMillis) {
    if (inFlightMap == null) {
      return true;
    }
    boolean[] claimed = {false};
    long now = System.currentTimeMillis();
    inFlightMap.compute(
        cacheKey,
        (key, current) -> {
          if (current == null
              || !current.isBuilding()
              || isStaleClaim(current, staleBuildingMillis)) {
            claimed[0] = true;
            return KeyAspectEntityCountInFlightStatus.building(now, claimantId);
          }
          return current;
        });
    return claimed[0];
  }

  void releaseInFlight(@Nonnull KeyAspectEntityCountCacheKey cacheKey) {
    if (inFlightMap == null) {
      return;
    }
    inFlightMap.compute(
        cacheKey, (key, current) -> current != null && current.isBuilding() ? null : current);
  }

  private static boolean isStaleClaim(
      @Nonnull KeyAspectEntityCountInFlightStatus current, long staleBuildingMillis) {
    return System.currentTimeMillis() - current.getRecordedAtMillis() > staleBuildingMillis;
  }

  private static void sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @FunctionalInterface
  public interface CountLoader {
    @Nonnull
    KeyAspectEntityCountResult load();
  }
}
