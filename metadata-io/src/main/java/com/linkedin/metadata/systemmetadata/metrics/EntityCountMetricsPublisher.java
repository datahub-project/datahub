package com.linkedin.metadata.systemmetadata.metrics;

import com.linkedin.metadata.config.EntityCountMetricsConfiguration;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountService;
import io.datahubproject.metadata.context.OperationContext;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Periodically refreshes entity counts and publishes them to registered sinks. */
@Slf4j
public class EntityCountMetricsPublisher implements AutoCloseable {

  private final KeyAspectEntityCountService keyAspectEntityCountService;
  private final OperationContext systemOperationContext;
  private final EntityCountMetricsSink sink;
  @Nullable private final MicrometerEntityCountMetricsSink micrometerLifecycleSink;
  private final EntityCountMetricsConfiguration config;
  private final ScheduledExecutorService scheduler;

  public EntityCountMetricsPublisher(
      @Nonnull KeyAspectEntityCountService keyAspectEntityCountService,
      @Nonnull OperationContext systemOperationContext,
      @Nonnull EntityCountMetricsSink sink,
      @Nullable MicrometerEntityCountMetricsSink micrometerLifecycleSink,
      @Nonnull EntityCountMetricsConfiguration config) {
    this.keyAspectEntityCountService = keyAspectEntityCountService;
    this.systemOperationContext = systemOperationContext;
    this.sink = sink;
    this.micrometerLifecycleSink = micrometerLifecycleSink;
    this.config = config;
    this.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "entity-count-metrics");
              thread.setDaemon(true);
              return thread;
            });

    long initialDelaySeconds = Math.max(0, config.getInitialDelaySeconds());
    long intervalSeconds = Math.max(0, config.getUpdateIntervalSeconds());
    if (intervalSeconds > 0) {
      scheduler.scheduleAtFixedRate(
          this::refresh, initialDelaySeconds, intervalSeconds, TimeUnit.SECONDS);
      log.info(
          "Scheduled entity count metrics refresh every {}s (initial delay {}s)",
          intervalSeconds,
          initialDelaySeconds);
    } else {
      scheduler.schedule(this::refresh, initialDelaySeconds, TimeUnit.SECONDS);
      log.info(
          "Scheduled one-shot entity count metrics refresh in {}s (updateIntervalSeconds=0)",
          initialDelaySeconds);
    }
  }

  void refresh() {
    Timer.Sample sample = micrometerLifecycleSink != null ? Timer.start() : null;
    try {
      KeyAspectEntityCountResult result =
          keyAspectEntityCountService.getCounts(systemOperationContext, null, config.isSkipCache());
      sink.publish(result);
      if (micrometerLifecycleSink != null) {
        micrometerLifecycleSink.recordRefreshSuccess();
      }
      log.debug(
          "Refreshed entity count metrics for {} entity types (cacheHit={})",
          result.getCounts().size(),
          result.isCacheHit());
    } catch (RuntimeException e) {
      if (micrometerLifecycleSink != null) {
        micrometerLifecycleSink.recordRefreshError();
      }
      log.warn("Entity count metrics refresh failed", e);
    } finally {
      if (sample != null && micrometerLifecycleSink != null) {
        sample.stop(micrometerLifecycleSink.refreshDuration());
      }
    }
  }

  @Override
  public void close() {
    scheduler.shutdownNow();
  }
}
