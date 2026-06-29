package com.linkedin.gms.factory.system_telemetry;

import com.linkedin.metadata.utils.metrics.MicrometerMetricsRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.ForkJoinPool;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

/**
 * Instruments the JVM-wide {@link ForkJoinPool#commonPool()} with Micrometer executor metrics under
 * the meter name {@code fork-join-common}.
 *
 * <p>The common pool is shared process-wide: it backs parallel streams, every {@link
 * java.util.concurrent.CompletableFuture} submitted without an explicit executor, and — when {@code
 * graphQL.concurrency.separateThreadPool} is off — GraphQL resolver execution via {@code
 * GraphQLConcurrencyUtils}. Because it is shared rather than owned by any one subsystem, it is
 * named for what it is ({@code fork-join-common}) rather than for a single caller, and is
 * registered unconditionally — independent of the GraphQL concurrency flag.
 *
 * <p>Micrometer's ForkJoinPool instrumentation emits {@code executor_queued}, {@code
 * executor_steals_total}, {@code executor_active}, {@code executor_running}, {@code
 * executor_parallelism_threads}, and {@code executor_pool_size_threads} (note: the queued gauge is
 * {@code executor_queued}, not the {@code executor_queued_tasks} emitted for ThreadPoolExecutors).
 * {@code executor_seconds} and {@code executor_idle_seconds} are registered as empty summaries but
 * remain at zero — ForkJoinPoolMetrics has no task-timing instrumentation. {@code
 * executor_scheduled_once_total} and {@code executor_scheduled_repetitively_total} are also zero
 * because ForkJoinPool does not implement {@link java.util.concurrent.ScheduledExecutorService}.
 */
@Slf4j
@Configuration
public class CommonForkJoinPoolMetricsFactory {

  /**
   * @param meterRegistry registry where the executor meters land. Autowired by Spring.
   */
  @Autowired
  public CommonForkJoinPoolMetricsFactory(final MeterRegistry meterRegistry) {
    final boolean registered =
        MicrometerMetricsRegistry.registerExecutorMetrics(
            "fork-join-common", ForkJoinPool.commonPool(), meterRegistry);
    if (registered) {
      log.info("Registered executor metrics for the common ForkJoinPool as 'fork-join-common'");
    }
  }
}
