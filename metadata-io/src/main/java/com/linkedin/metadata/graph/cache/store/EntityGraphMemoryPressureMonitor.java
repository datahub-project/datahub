package com.linkedin.metadata.graph.cache.store;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties;
import com.linkedin.metadata.graph.cache.config.EntityGraphRegistry;
import com.linkedin.metadata.graph.cache.service.EntityGraphCacheService;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntityGraphMemoryPressureMonitor implements AutoCloseable {

  private final EntityGraphCacheProperties.MemoryPressure config;
  private final EntityGraphCacheService cacheService;
  private final EntityGraphRegistry registry;
  private final ScheduledExecutorService executor;
  private volatile boolean underPressure;
  private volatile long lastEvictionMillis;

  public EntityGraphMemoryPressureMonitor(
      @Nonnull EntityGraphCacheProperties properties,
      @Nonnull EntityGraphCacheService cacheService,
      @Nonnull EntityGraphRegistry registry) {
    this.config = properties.getEviction().getMemoryPressure();
    this.cacheService = cacheService;
    this.registry = registry;
    this.executor =
        config.isEnabled()
            ? Executors.newSingleThreadScheduledExecutor(
                r -> {
                  Thread t = new Thread(r, "entity-graph-memory-pressure");
                  t.setDaemon(true);
                  return t;
                })
            : null;
    if (executor != null) {
      executor.scheduleAtFixedRate(
          this::checkHeap,
          config.getCheckIntervalSeconds(),
          config.getCheckIntervalSeconds(),
          TimeUnit.SECONDS);
    }
  }

  private void checkHeap() {
    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    long used = memoryBean.getHeapMemoryUsage().getUsed();
    long max = memoryBean.getHeapMemoryUsage().getMax();
    if (max <= 0) {
      return;
    }
    int usedPercent = (int) ((used * 100) / max);
    underPressure =
        updateUnderPressure(
            underPressure,
            usedPercent,
            config.getHeapUsageThresholdPercent(),
            config.getHysteresisPercent());
    if (!underPressure) {
      return;
    }
    long now = System.currentTimeMillis();
    if (now - lastEvictionMillis < config.getCooldownSeconds() * 1000L) {
      return;
    }
    lastEvictionMillis = now;
    log.warn(
        "Entity graph memory pressure {}% >= {}% (clear below {}%), action={}",
        usedPercent,
        config.getHeapUsageThresholdPercent(),
        clearThresholdPercent(config.getHeapUsageThresholdPercent(), config.getHysteresisPercent()),
        config.getAction());
    switch (config.getAction()) {
      case "EVICT_ALL_LOCAL":
        cacheService.getLocalViews().evictAll();
        break;
      case "EVICT_LOCAL_LRU":
      default:
        for (String graphId : registry.getGraphsById().keySet()) {
          cacheService.getLocalViews().evictLruForGraph(graphId, 1);
        }
        break;
    }
  }

  /**
   * Applies threshold hysteresis: enter pressure at {@code triggerPercent}, exit only below {@code
   * triggerPercent - hysteresisPercent}. Between the two bounds, the prior state is held.
   */
  static boolean updateUnderPressure(
      boolean currentlyUnderPressure, int usedPercent, int triggerPercent, int hysteresisPercent) {
    if (usedPercent >= triggerPercent) {
      return true;
    }
    if (usedPercent < clearThresholdPercent(triggerPercent, hysteresisPercent)) {
      return false;
    }
    return currentlyUnderPressure;
  }

  static int clearThresholdPercent(int triggerPercent, int hysteresisPercent) {
    return Math.max(0, triggerPercent - hysteresisPercent);
  }

  @PreDestroy
  @Override
  public void close() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }
}
