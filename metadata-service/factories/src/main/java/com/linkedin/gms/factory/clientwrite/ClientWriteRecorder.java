package com.linkedin.gms.factory.clientwrite;

import com.linkedin.metadata.config.ClientWriteTrackingConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
 * Records client writes two ways. The {@code gms.client_writes} counter carries only bounded tags
 * (client, mode, version, API, whether the actor is the system principal). Who sent the writes —
 * the actor and User-Agent — would explode metric cardinality, so per-actor totals are instead
 * accumulated in memory and written as one log line per combination each summary interval.
 *
 * <p>The summary is written by a single background thread, never by the request thread that records
 * a write, and once more when the recorder is closed on shutdown.
 */
@Slf4j
public class ClientWriteRecorder implements AutoCloseable {
  static final String METRIC_NAME = "gms.client_writes";
  static final String SUMMARY_PREFIX = "client_write_summary";
  static final String ANONYMOUS_ACTOR = "anonymous";

  // Caps distinct client_version tag values; anyone can send any version string.
  static final int MAX_VERSION_TAGS = 100;
  private static final int MAX_ACTOR_LENGTH = 200;
  private static final String SUMMARY_THREAD_NAME = "client-write-summary";
  private static final int SHUTDOWN_TIMEOUT_SECONDS = 5;

  @Nullable private final MeterRegistry meterRegistry;
  private final boolean summaryEnabled;
  private final int maxTrackedKeys;
  private final LongSupplier clockMillis;
  private final Consumer<String> summarySink;
  @Nullable private final ScheduledExecutorService summaryScheduler;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final Set<String> versionTags = ConcurrentHashMap.newKeySet();
  private final ConcurrentHashMap<SummaryKey, LongAdder> counts = new ConcurrentHashMap<>();
  private final LongAdder untracked = new LongAdder();
  private long windowStartMillis;

  /**
   * Starts the summary thread when tracking is enabled and {@code summaryIntervalSeconds} is
   * positive. A non-positive interval turns the summary off; the metric is still recorded.
   */
  public ClientWriteRecorder(
      @Nullable MeterRegistry meterRegistry,
      @Nonnull ClientWriteTrackingConfiguration configuration) {
    this(
        meterRegistry,
        configuration.isEnabled() && configuration.getSummaryIntervalSeconds() > 0,
        configuration.getMaxTrackedKeys(),
        System::currentTimeMillis,
        log::info,
        configuration.getSummaryIntervalSeconds());
    if (configuration.isEnabled() && !summaryEnabled) {
      log.info(
          "Client write summary is disabled (summaryIntervalSeconds={}); the {} metric is still"
              + " recorded",
          configuration.getSummaryIntervalSeconds(),
          METRIC_NAME);
    }
  }

  /** Without a scheduler: tests call {@link #flush()} directly. */
  ClientWriteRecorder(
      @Nullable MeterRegistry meterRegistry,
      boolean summaryEnabled,
      int maxTrackedKeys,
      @Nonnull LongSupplier clockMillis,
      @Nonnull Consumer<String> summarySink) {
    this(meterRegistry, summaryEnabled, maxTrackedKeys, clockMillis, summarySink, 0);
  }

  private ClientWriteRecorder(
      @Nullable MeterRegistry meterRegistry,
      boolean summaryEnabled,
      int maxTrackedKeys,
      @Nonnull LongSupplier clockMillis,
      @Nonnull Consumer<String> summarySink,
      int scheduleIntervalSeconds) {
    this.meterRegistry = meterRegistry;
    this.summaryEnabled = summaryEnabled;
    this.maxTrackedKeys = Math.max(0, maxTrackedKeys);
    this.clockMillis = clockMillis;
    this.summarySink = summarySink;
    this.windowStartMillis = clockMillis.getAsLong();
    if (summaryEnabled && scheduleIntervalSeconds > 0) {
      summaryScheduler =
          Executors.newSingleThreadScheduledExecutor(
              r -> {
                Thread t = new Thread(r, SUMMARY_THREAD_NAME);
                // Daemon so a missed close() never holds the JVM open; close() is the normal path.
                t.setDaemon(true);
                return t;
              });
      summaryScheduler.scheduleAtFixedRate(
          this::flushQuietly, scheduleIntervalSeconds, scheduleIntervalSeconds, TimeUnit.SECONDS);
    } else {
      summaryScheduler = null;
    }
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
    if (!summaryEnabled) {
      return;
    }

    SummaryKey key =
        new SummaryKey(
            actorUrn == null
                ? ANONYMOUS_ACTOR
                : ClientWriteClassifier.sanitizeForLog(actorUrn, MAX_ACTOR_LENGTH),
            write);
    LongAdder count = counts.get(key);
    if (count == null) {
      if (counts.size() >= maxTrackedKeys) {
        untracked.increment();
      } else {
        count = counts.computeIfAbsent(key, k -> new LongAdder());
      }
    }
    if (count != null) {
      count.increment();
    }
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
      LongAdder count = counts.remove(key);
      if (count == null) {
        continue;
      }
      long total = count.sum();
      if (total > 0) {
        summarySink.accept(summaryLine(key, total, windowSeconds));
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
  private static String summaryLine(@Nonnull SummaryKey key, long total, long windowSeconds) {
    ClientWrite write = key.getWrite();
    return String.format(
        "%s actor=%s client=%s client_version=%s client_mode=%s api=%s count=%d"
            + " window_seconds=%d user_agent=\"%s\"",
        SUMMARY_PREFIX,
        key.getActor(),
        write.getClient(),
        write.getClientVersion(),
        write.getClientMode(),
        write.getApi(),
        total,
        windowSeconds,
        write.getUserAgent());
  }

  @Nonnull
  private String versionTag(@Nonnull String version) {
    if (ClientWriteClassifier.NONE.equals(version)
        || ClientWriteClassifier.UNPARSEABLE.equals(version)
        || versionTags.contains(version)) {
      return version;
    }
    if (versionTags.size() < MAX_VERSION_TAGS && versionTags.add(version)) {
      return version;
    }
    return versionTags.contains(version) ? version : ClientWriteClassifier.OTHER;
  }

  @Value
  private static class SummaryKey {
    String actor;
    ClientWrite write;
  }
}
