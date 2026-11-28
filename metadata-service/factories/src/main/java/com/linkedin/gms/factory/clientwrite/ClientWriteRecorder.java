package com.linkedin.gms.factory.clientwrite;

import com.linkedin.metadata.config.ClientWriteTrackingConfiguration;
import com.linkedin.metadata.utils.metrics.MicrometerMetricsRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Records accepted client writes two ways. The {@code gms.client_writes} counter carries only
 * bounded tags (client, mode, version, API, whether the actor is the system principal). Who sent
 * the writes would explode metric cardinality, so per-actor totals are instead accumulated in
 * memory, keyed by actor, client, mode, version and API, and written as one {@code
 * client_write_summary} log line per key each summary interval, with the first User-Agent seen for
 * that key as a sample.
 *
 * <p>The {@code client_version} tag keeps the first {@code maxVersionTags} distinct versions a pod
 * sees; later versions are tagged {@code other} until the pod restarts.
 *
 * <p>The summary is written by a single background thread, never by the request thread that records
 * a write, and once more when the recorder is closed on shutdown. Writes recorded after {@link
 * #close()} still count in the metric but are left out of the summary, which has been written.
 *
 * <p>This is standalone rather than built on the usage aggregation subsystem for two reasons. Usage
 * aggregation is off by default ({@code USAGE_AGGREGATION_ENABLED=false}), while this tracking must
 * be on by default to find out which clients write. And it exports the actor of each write in a
 * periodic log, which usage aggregation deliberately does not: it reports only a distinct count of
 * identities.
 */
@Slf4j
public class ClientWriteRecorder implements AutoCloseable {
  static final String METRIC_NAME = "gms.client_writes";
  static final String SUMMARY_PREFIX = "client_write_summary";
  static final String ANONYMOUS_ACTOR = "anonymous";

  private static final int MAX_ACTOR_LENGTH = 200;
  private static final String SUMMARY_THREAD_NAME = "client-write-summary";
  private static final int SHUTDOWN_TIMEOUT_SECONDS = 5;

  @Nullable private final MeterRegistry meterRegistry;
  private final boolean summaryEnabled;
  private final int maxTrackedKeys;
  private final int maxVersionTags;
  private final LongSupplier clockMillis;
  private final Consumer<String> summarySink;
  @Nullable private final ScheduledExecutorService summaryScheduler;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final Set<String> versionTags = ConcurrentHashMap.newKeySet();
  private final ConcurrentHashMap<SummaryKey, SummaryCount> counts = new ConcurrentHashMap<>();
  private final LongAdder untracked = new LongAdder();
  private long windowStartMillis;

  /**
   * Starts the summary thread when tracking is enabled, {@code summaryIntervalSeconds} is positive
   * and {@code maxTrackedKeys} is positive. Otherwise the summary is off; the metric is still
   * recorded. A negative {@code maxTrackedKeys} or {@code maxVersionTags} is treated as 0.
   */
  public ClientWriteRecorder(
      @Nullable MeterRegistry meterRegistry,
      @Nonnull ClientWriteTrackingConfiguration configuration) {
    this(
        meterRegistry,
        configuration.isEnabled()
            && configuration.getSummaryIntervalSeconds() > 0
            && configuration.getMaxTrackedKeys() > 0,
        nonNegative("maxTrackedKeys", configuration.getMaxTrackedKeys()),
        nonNegative("maxVersionTags", configuration.getMaxVersionTags()),
        System::currentTimeMillis,
        log::info,
        configuration.getSummaryIntervalSeconds());
    log.info(
        "Client write tracking: enabled={} summaryEnabled={} summaryIntervalSeconds={}"
            + " maxTrackedKeys={} maxVersionTags={}",
        configuration.isEnabled(),
        summaryEnabled,
        configuration.getSummaryIntervalSeconds(),
        maxTrackedKeys,
        maxVersionTags);
  }

  /** Without a scheduler: tests call {@link #flush()} directly. */
  ClientWriteRecorder(
      @Nullable MeterRegistry meterRegistry,
      boolean summaryEnabled,
      int maxTrackedKeys,
      int maxVersionTags,
      @Nonnull LongSupplier clockMillis,
      @Nonnull Consumer<String> summarySink) {
    this(
        meterRegistry, summaryEnabled, maxTrackedKeys, maxVersionTags, clockMillis, summarySink, 0);
  }

  private ClientWriteRecorder(
      @Nullable MeterRegistry meterRegistry,
      boolean summaryEnabled,
      int maxTrackedKeys,
      int maxVersionTags,
      @Nonnull LongSupplier clockMillis,
      @Nonnull Consumer<String> summarySink,
      int scheduleIntervalSeconds) {
    this.meterRegistry = meterRegistry;
    this.summaryEnabled = summaryEnabled;
    this.maxTrackedKeys = Math.max(0, maxTrackedKeys);
    this.maxVersionTags = Math.max(0, maxVersionTags);
    this.clockMillis = clockMillis;
    this.summarySink = summarySink;
    this.windowStartMillis = clockMillis.getAsLong();
    if (summaryEnabled && scheduleIntervalSeconds > 0) {
      summaryScheduler = monitored(meterRegistry, newSummaryScheduler());
      summaryScheduler.scheduleAtFixedRate(
          this::flushQuietly, scheduleIntervalSeconds, scheduleIntervalSeconds, TimeUnit.SECONDS);
    } else {
      summaryScheduler = null;
    }
  }

  private static int nonNegative(@Nonnull String property, int value) {
    if (value < 0) {
      log.warn("Client write tracking {}={} is negative; using 0", property, value);
      return 0;
    }
    return value;
  }

  @Nonnull
  private static ScheduledExecutorService newSummaryScheduler() {
    return Executors.newSingleThreadScheduledExecutor(
        r -> {
          Thread t = new Thread(r, SUMMARY_THREAD_NAME);
          // Daemon so a missed close() never holds the JVM open; close() is the normal path.
          t.setDaemon(true);
          return t;
        });
  }

  @Nonnull
  private static ScheduledExecutorService monitored(
      @Nullable MeterRegistry meterRegistry, @Nonnull ScheduledExecutorService scheduler) {
    ExecutorService monitored =
        MicrometerMetricsRegistry.monitorExecutor(SUMMARY_THREAD_NAME, scheduler, meterRegistry);
    // Micrometer wraps a scheduled executor in a scheduled executor; fall back if it ever does not.
    return monitored instanceof ScheduledExecutorService
        ? (ScheduledExecutorService) monitored
        : scheduler;
  }

  /** Whether a background thread writes the summary. */
  boolean isSummaryScheduled() {
    return summaryScheduler != null;
  }

  /** Counts one write. Runs on the request thread, so it only updates counters and never logs. */
  public void record(@Nonnull ClientWrite write, @Nullable String actorUrn, boolean systemActor) {
    if (meterRegistry != null) {
      meterRegistry
          .counter(
              METRIC_NAME,
              Tags.of(
                  "client", write.getClient(),
                  "client_mode", write.getClientMode(),
                  "client_version", versionTag(write.getClientVersion()),
                  "api", write.getApi(),
                  "system_actor", Boolean.toString(systemActor)))
          .increment();
    }
    // After close() the final summary has been written, so later writes count in the metric only.
    if (!summaryEnabled || closed.get()) {
      return;
    }

    SummaryKey key =
        new SummaryKey(
            actorUrn == null
                ? ANONYMOUS_ACTOR
                : ClientWriteClassifier.sanitizeForLog(actorUrn, MAX_ACTOR_LENGTH),
            write.getApi(),
            write.getClient(),
            write.getClientMode(),
            write.getClientVersion());
    SummaryCount count = counts.get(key);
    if (count == null) {
      if (counts.size() >= maxTrackedKeys) {
        untracked.increment();
        return;
      }
      count = counts.computeIfAbsent(key, k -> new SummaryCount(write.getUserAgent()));
    }
    count.count.increment();
  }

  /**
   * Writes one summary line per actor and client combination seen since the last flush, then starts
   * a new window. Runs on the summary thread and on close, never on a request thread.
   */
  synchronized void flush() {
    if (!summaryEnabled) {
      return;
    }
    long now = clockMillis.getAsLong();
    long windowSeconds = (now - windowStartMillis) / 1000;
    windowStartMillis = now;
    // A write that lands on a counter between its removal and the sum below is left out of this
    // summary. The metric still counts it; the summary is for attribution, not exact totals.
    for (SummaryKey key : counts.keySet()) {
      SummaryCount count = counts.remove(key);
      if (count == null) {
        continue;
      }
      long total = count.count.sum();
      if (total > 0) {
        summarySink.accept(summaryLine(key, count.userAgent, total, windowSeconds));
      }
    }
    long dropped = untracked.sumThenReset();
    if (dropped > 0) {
      summarySink.accept(
          String.format(
              "%s untracked=%d window_seconds=%d max_tracked_keys=%d",
              SUMMARY_PREFIX, dropped, windowSeconds, maxTrackedKeys));
    }
  }

  private void flushQuietly() {
    // An exception escaping a scheduleAtFixedRate task cancels every later run.
    try {
      flush();
    } catch (RuntimeException e) {
      log.warn("Failed to write the client write summary", e);
    }
  }

  /** Stops the summary thread and writes the totals of the final, partial window. */
  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    if (summaryScheduler != null) {
      summaryScheduler.shutdown();
      try {
        if (!summaryScheduler.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          summaryScheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        summaryScheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    flushQuietly();
  }

  @Nonnull
  private static String summaryLine(
      @Nonnull SummaryKey key, @Nonnull String userAgent, long total, long windowSeconds) {
    return String.format(
        "%s actor=%s client=%s client_version=%s client_mode=%s api=%s count=%d"
            + " window_seconds=%d user_agent=\"%s\"",
        SUMMARY_PREFIX,
        key.getActor(),
        key.getClient(),
        key.getClientVersion(),
        key.getClientMode(),
        key.getApi(),
        total,
        windowSeconds,
        userAgent);
  }

  @Nonnull
  private String versionTag(@Nonnull String version) {
    if (ClientWriteClassifier.NONE.equals(version)
        || ClientWriteClassifier.UNPARSEABLE.equals(version)
        || versionTags.contains(version)) {
      return version;
    }
    if (versionTags.size() < maxVersionTags && versionTags.add(version)) {
      return version;
    }
    return versionTags.contains(version) ? version : ClientWriteClassifier.OTHER;
  }

  /** The User-Agent is not part of the key, so a client that varies it cannot fill the map. */
  @Value
  private static class SummaryKey {
    String actor;
    String api;
    String client;
    String clientMode;
    String clientVersion;
  }

  private static final class SummaryCount {
    final LongAdder count = new LongAdder();
    // The first User-Agent seen for the key in this window, already sanitized for the log.
    final String userAgent;

    SummaryCount(@Nonnull String userAgent) {
      this.userAgent = userAgent;
    }
  }
}
