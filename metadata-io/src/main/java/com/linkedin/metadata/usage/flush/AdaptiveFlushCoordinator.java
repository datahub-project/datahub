package com.linkedin.metadata.usage.flush;

import com.linkedin.metadata.usage.store.InMemoryUsageAggregationStore;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Adaptive flush coordinator: scheduled, max window, and cardinality triggers. */
@Slf4j
public class AdaptiveFlushCoordinator implements AutoCloseable {

  private final InMemoryUsageAggregationStore store;
  private final long scheduledIntervalSeconds;
  private final ScheduledExecutorService scheduler;

  public AdaptiveFlushCoordinator(
      @Nonnull InMemoryUsageAggregationStore store, long scheduledIntervalSeconds) {
    this.store = store;
    this.scheduledIntervalSeconds = scheduledIntervalSeconds;
    this.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "usage-aggregation-flush");
              thread.setDaemon(true);
              return thread;
            });
    if (scheduledIntervalSeconds > 0) {
      scheduler.scheduleAtFixedRate(
          this::tick, scheduledIntervalSeconds, scheduledIntervalSeconds, TimeUnit.SECONDS);
    }
  }

  private void tick() {
    try {
      if (store.isWindowExpired()) {
        store.flush(FlushTrigger.MAX_WINDOW);
      } else {
        store.flush(FlushTrigger.SCHEDULED);
      }
    } catch (RuntimeException e) {
      log.warn("Scheduled usage aggregation flush failed", e);
    }
  }

  public void shutdown() {
    try {
      store.flush(FlushTrigger.SHUTDOWN);
    } catch (RuntimeException e) {
      log.warn("Shutdown usage aggregation flush failed", e);
    }
    scheduler.shutdownNow();
  }

  @Override
  public void close() {
    shutdown();
  }
}
