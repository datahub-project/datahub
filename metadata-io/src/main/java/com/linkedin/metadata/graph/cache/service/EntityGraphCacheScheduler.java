package com.linkedin.metadata.graph.cache.service;

import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphRegistry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntityGraphCacheScheduler implements AutoCloseable {

  private final EntityGraphRegistry registry;
  private final EntityGraphCacheService cacheService;
  private final ScheduledExecutorService executor;

  public EntityGraphCacheScheduler(
      @Nonnull EntityGraphRegistry registry, @Nonnull EntityGraphCacheService cacheService) {
    this.registry = registry;
    this.cacheService = cacheService;
    this.executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "entity-graph-scheduler");
              t.setDaemon(true);
              return t;
            });

    for (EntityGraphDefinition graph : registry.getScheduledFullGraphs()) {
      int interval = Math.max(5, graph.getPopulationIntervalSeconds());
      executor.scheduleAtFixedRate(
          () -> {
            try {
              cacheService.scheduledRebuild(graph);
            } catch (Exception e) {
              log.warn("Scheduled entity graph rebuild failed for {}", graph.getGraphId(), e);
            }
          },
          interval,
          interval,
          TimeUnit.SECONDS);
      log.info("Scheduled entity graph rebuild for {} every {}s", graph.getGraphId(), interval);
    }
  }

  @PreDestroy
  @Override
  public void close() {
    executor.shutdownNow();
  }
}
