package com.linkedin.metadata.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Shared cache that throttles timeseries aspect writes to Elasticsearch. A single cache entry per
 * URN holds last-write timestamps keyed by aspect name. A default refresh period applies to all
 * (entity, aspect) pairs unless overridden via a JSON map of entity &rarr; aspect &rarr; seconds.
 */
@Slf4j
public class TimeseriesWriteThrottleCache {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public enum ThrottleTarget {
    ENTITY_INDEX,
    TIMESERIES_INDEX
  }

  private final Cache<String, ConcurrentHashMap<String, Long>> cache;
  private final boolean entityIndexEnabled;
  private final boolean timeseriesIndexEnabled;
  private final boolean observeEnabled;
  private final long defaultRefreshPeriodMs;
  private final Map<String, Map<String, Long>> refreshOverridesMs;

  public TimeseriesWriteThrottleCache(@Nonnull TimeseriesWriteThrottleConfiguration config) {
    TimeseriesWriteThrottleConfiguration.IndexThrottleConfig entityCfg = config.getEntityIndex();
    TimeseriesWriteThrottleConfiguration.IndexThrottleConfig tsCfg = config.getTimeseriesIndex();
    TimeseriesWriteThrottleConfiguration.IndexThrottleConfig observeCfg = config.getObserve();

    this.entityIndexEnabled = entityCfg != null && entityCfg.isEnabled();
    this.timeseriesIndexEnabled = tsCfg != null && tsCfg.isEnabled();
    this.observeEnabled = observeCfg != null && observeCfg.isEnabled();
    this.defaultRefreshPeriodMs = config.getRefreshPeriodSeconds() * 1000L;
    this.refreshOverridesMs = parseOverrides(config.getRefreshOverrides());

    long maxRefreshSeconds = computeMaxRefreshSeconds(config);
    this.cache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(maxRefreshSeconds, TimeUnit.SECONDS)
            .maximumSize(config.getMaxCacheUrns())
            .build();

    log.info(
        "TimeseriesWriteThrottleCache initialized: entityIndex={}, timeseriesIndex={}, "
            + "observe={}, defaultRefreshPeriod={}s, overrides={}, cacheTtl={}s, maxUrns={}",
        entityIndexEnabled,
        timeseriesIndexEnabled,
        observeEnabled,
        config.getRefreshPeriodSeconds(),
        config.getRefreshOverrides(),
        maxRefreshSeconds,
        config.getMaxCacheUrns());
  }

  /** Whether any throttle target (including observe-only) is enabled. */
  public boolean isEnabled() {
    return entityIndexEnabled || timeseriesIndexEnabled || observeEnabled;
  }

  public boolean isEntityIndexEnabled() {
    return entityIndexEnabled;
  }

  public boolean isTimeseriesIndexEnabled() {
    return timeseriesIndexEnabled;
  }

  /**
   * Whether log-only observe mode is enabled (tracks throttle-eligible writes without suppressing).
   */
  public boolean isObserveEnabled() {
    return observeEnabled;
  }

  /**
   * Returns {@code true} if the (URN, aspect) pair is within the refresh period and should be
   * throttled. The refresh period is resolved per (entityName, aspectName) with a fallback to the
   * default.
   */
  public boolean shouldThrottle(
      @Nonnull String entityName,
      @Nonnull String urnStr,
      @Nonnull String aspectName,
      long eventTimeMs) {

    if (!isEnabled()) {
      return false;
    }

    ConcurrentHashMap<String, Long> timestamps = cache.getIfPresent(urnStr);
    if (timestamps == null) {
      return false;
    }

    Long lastWriteMs = timestamps.get(aspectName);
    if (lastWriteMs == null) {
      return false;
    }

    long refreshMs = getRefreshPeriodMs(entityName, aspectName);
    return eventTimeMs > lastWriteMs && (eventTimeMs - lastWriteMs) < refreshMs;
  }

  /**
   * Records a successful write so subsequent events within the refresh period are throttled.
   *
   * @param urnStr the entity URN string
   * @param aspectName the timeseries aspect name
   * @param eventTimeMs the event timestamp (epoch ms) written
   */
  public void recordWrite(@Nonnull String urnStr, @Nonnull String aspectName, long eventTimeMs) {

    if (!isEnabled()) {
      return;
    }

    cache
        .asMap()
        .compute(
            urnStr,
            (urn, existing) -> {
              ConcurrentHashMap<String, Long> map =
                  existing != null ? existing : new ConcurrentHashMap<>();
              map.merge(aspectName, eventTimeMs, Math::max);
              return map;
            });
  }

  @Nonnull
  public ThrottleSummary newSummary() {
    return new ThrottleSummary();
  }

  @VisibleForTesting
  long getRefreshPeriodMs(@Nonnull String entityName, @Nonnull String aspectName) {
    Map<String, Long> entityOverrides = refreshOverridesMs.get(entityName);
    if (entityOverrides != null) {
      Long overrideMs = entityOverrides.get(aspectName);
      if (overrideMs != null) {
        return overrideMs;
      }
    }
    return defaultRefreshPeriodMs;
  }

  @VisibleForTesting
  Cache<String, ConcurrentHashMap<String, Long>> getCache() {
    return cache;
  }

  private static Map<String, Map<String, Long>> parseOverrides(String json) {
    if (json == null || json.isBlank()) {
      return Collections.emptyMap();
    }
    try {
      Map<String, Map<String, Long>> parsed =
          MAPPER.readValue(json, new TypeReference<Map<String, Map<String, Long>>>() {});
      // Convert seconds to millis
      ConcurrentHashMap<String, Map<String, Long>> result = new ConcurrentHashMap<>();
      for (Map.Entry<String, Map<String, Long>> entityEntry : parsed.entrySet()) {
        ConcurrentHashMap<String, Long> aspectMap = new ConcurrentHashMap<>();
        for (Map.Entry<String, Long> aspectEntry : entityEntry.getValue().entrySet()) {
          aspectMap.put(aspectEntry.getKey(), aspectEntry.getValue() * 1000L);
        }
        result.put(entityEntry.getKey(), aspectMap);
      }
      return result;
    } catch (Exception e) {
      log.warn("Failed to parse refreshOverrides JSON '{}', using defaults only", json, e);
      return Collections.emptyMap();
    }
  }

  private static long computeMaxRefreshSeconds(TimeseriesWriteThrottleConfiguration config) {
    long max = config.getRefreshPeriodSeconds();
    try {
      if (config.getRefreshOverrides() != null && !config.getRefreshOverrides().isBlank()) {
        Map<String, Map<String, Long>> parsed =
            MAPPER.readValue(
                config.getRefreshOverrides(),
                new TypeReference<Map<String, Map<String, Long>>>() {});
        for (Map<String, Long> aspects : parsed.values()) {
          for (Long seconds : aspects.values()) {
            max = Math.max(max, seconds);
          }
        }
      }
    } catch (Exception e) {
      // already warned in parseOverrides
    }
    return max;
  }

  /** Accumulates throttle decisions within a single batch and logs a summary at the end. */
  public static class ThrottleSummary {
    private int entityIndexSuppressed;
    private int timeseriesIndexSuppressed;
    private int entityIndexWritten;
    private int timeseriesIndexWritten;
    private int observed;

    public void recordSuppressed(@Nonnull ThrottleTarget target) {
      switch (target) {
        case ENTITY_INDEX:
          entityIndexSuppressed++;
          break;
        case TIMESERIES_INDEX:
          timeseriesIndexSuppressed++;
          break;
      }
    }

    public void recordWritten(@Nonnull ThrottleTarget target) {
      switch (target) {
        case ENTITY_INDEX:
          entityIndexWritten++;
          break;
        case TIMESERIES_INDEX:
          timeseriesIndexWritten++;
          break;
      }
    }

    /** Records an event that would have been throttled (observe/log-only mode). */
    public void recordObserved() {
      observed++;
    }

    public int getEntityIndexSuppressed() {
      return entityIndexSuppressed;
    }

    public int getTimeseriesIndexSuppressed() {
      return timeseriesIndexSuppressed;
    }

    public int getEntityIndexWritten() {
      return entityIndexWritten;
    }

    public int getTimeseriesIndexWritten() {
      return timeseriesIndexWritten;
    }

    public int getObserved() {
      return observed;
    }

    /** Logs summary lines for suppressed writes and/or observed throttle-eligible events. */
    public void logIfSuppressed() {
      int totalSuppressed = entityIndexSuppressed + timeseriesIndexSuppressed;
      if (totalSuppressed > 0) {
        int totalEvents =
            entityIndexSuppressed
                + entityIndexWritten
                + timeseriesIndexSuppressed
                + timeseriesIndexWritten;
        log.warn(
            "Timeseries throttle: suppressed {} entity-index writes, {} timeseries-index writes "
                + "(passed {} entity-index, {} timeseries-index) in batch of {} timeseries events",
            entityIndexSuppressed,
            timeseriesIndexSuppressed,
            entityIndexWritten,
            timeseriesIndexWritten,
            totalEvents);
      }
      if (observed > 0) {
        log.warn(
            "Timeseries throttle [observe]: {} writes would have been throttled "
                + "(no writes suppressed — observe mode only)",
            observed);
      }
    }
  }
}
