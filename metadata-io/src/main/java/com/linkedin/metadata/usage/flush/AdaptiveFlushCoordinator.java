package com.linkedin.metadata.usage.flush;

import com.linkedin.metadata.usage.store.InMemoryUsageAggregationStore;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Adaptive flush coordinator: scheduled, max window, and cardinality triggers. */
@Slf4j
public class AdaptiveFlushCoordinator implements AutoCloseable {

  private static final ZoneOffset ALIGNMENT_ZONE = ZoneOffset.UTC;

  private final InMemoryUsageAggregationStore store;
  // Scheduled (cron) flushes have no ambient request context, so the coordinator carries the
  // system context and supplies it to every store.flush(...). The coordinator is not on the
  // systemOperationContext bean construction graph, so holding it here introduces no cycle
  // (unlike injecting it into a flush sink, which is reachable from that graph via the store).
  private final OperationContext systemOperationContext;
  private final long scheduledIntervalSeconds;
  private final Duration boundarySkew;
  private final Clock clock;
  private final ScheduledExecutorService scheduler;
  private volatile long lastScheduledFlushMillis;

  public AdaptiveFlushCoordinator(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull InMemoryUsageAggregationStore store,
      long scheduledIntervalSeconds) {
    this(systemOperationContext, store, scheduledIntervalSeconds, Clock.systemUTC());
  }

  public AdaptiveFlushCoordinator(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull InMemoryUsageAggregationStore store,
      long scheduledIntervalSeconds,
      @Nonnull Clock clock) {
    this(systemOperationContext, store, scheduledIntervalSeconds, clock, true);
  }

  AdaptiveFlushCoordinator(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull InMemoryUsageAggregationStore store,
      long scheduledIntervalSeconds,
      @Nonnull Clock clock,
      boolean enableSchedulers) {
    this.systemOperationContext = systemOperationContext;
    this.store = store;
    this.scheduledIntervalSeconds = scheduledIntervalSeconds;
    this.clock = clock;
    this.boundarySkew =
        scheduledIntervalSeconds > 0
            ? Duration.ofSeconds(scheduledIntervalSeconds)
            : Duration.ofSeconds(1);
    this.lastScheduledFlushMillis = clock.millis();
    this.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "usage-aggregation-flush");
              thread.setDaemon(true);
              return thread;
            });
    if (enableSchedulers && scheduledIntervalSeconds > 0) {
      scheduler.scheduleAtFixedRate(
          this::tick, scheduledIntervalSeconds, scheduledIntervalSeconds, TimeUnit.SECONDS);
    }
  }

  /** Package-visible for deterministic tests with a controllable {@link Clock}. */
  void tick() {
    try {
      if (store.isAlignmentEnabled() && shouldFlushForAlignmentBoundary()) {
        store.flush(systemOperationContext, FlushTrigger.SCHEDULED);
        lastScheduledFlushMillis = clock.millis();
        return;
      }
      if (store.isWindowExpired()) {
        store.flush(systemOperationContext, FlushTrigger.MAX_WINDOW);
        lastScheduledFlushMillis = clock.millis();
        return;
      }
      if (!store.isAlignmentEnabled()) {
        store.flush(systemOperationContext, FlushTrigger.SCHEDULED);
        lastScheduledFlushMillis = clock.millis();
        return;
      }
      if (scheduledIntervalElapsed()) {
        store.flush(systemOperationContext, FlushTrigger.SCHEDULED);
        lastScheduledFlushMillis = clock.millis();
      }
    } catch (RuntimeException e) {
      log.warn("Scheduled usage aggregation flush failed", e);
    }
  }

  private boolean scheduledIntervalElapsed() {
    return scheduledIntervalSeconds > 0
        && clock.millis() - lastScheduledFlushMillis
            >= TimeUnit.SECONDS.toMillis(scheduledIntervalSeconds);
  }

  private boolean shouldFlushForAlignmentBoundary() {
    Duration alignmentPeriod = store.alignmentPeriod();
    if (alignmentPeriod == null) {
      return false;
    }
    Instant now = clock.instant();
    Instant boundary =
        UsageFlushBoundaryUtils.nextBoundary(
            store.windowStartSnapshot(), alignmentPeriod, ALIGNMENT_ZONE);
    if (!now.isBefore(boundary)) {
      return true;
    }
    return !now.isBefore(boundary.minus(boundarySkew));
  }

  public void shutdown() {
    try {
      store.flush(systemOperationContext, FlushTrigger.SHUTDOWN);
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
