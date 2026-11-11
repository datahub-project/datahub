package com.linkedin.gms.factory.clientwrite;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
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
 */
@Slf4j
public class ClientWriteRecorder {
  static final String METRIC_NAME = "gms.client_writes";
  static final String SUMMARY_PREFIX = "client_write_summary";
  static final String ANONYMOUS_ACTOR = "anonymous";

  // Caps distinct client_version tag values; anyone can send any version string.
  static final int MAX_VERSION_TAGS = 100;
  private static final int MAX_ACTOR_LENGTH = 200;

  @Nullable private final MeterRegistry meterRegistry;
  private final long summaryIntervalMillis;
  private final int maxTrackedKeys;
  private final LongSupplier clockMillis;
  private final Consumer<String> summarySink;

  private final Set<String> versionTags = ConcurrentHashMap.newKeySet();
  private final ConcurrentHashMap<SummaryKey, LongAdder> counts = new ConcurrentHashMap<>();
  private final LongAdder untracked = new LongAdder();
  private final AtomicLong windowStartMillis;

  public ClientWriteRecorder(
      @Nullable MeterRegistry meterRegistry, int summaryIntervalSeconds, int maxTrackedKeys) {
    this(
        meterRegistry,
        Duration.ofSeconds(summaryIntervalSeconds),
        maxTrackedKeys,
        System::currentTimeMillis,
        log::info);
  }

  ClientWriteRecorder(
      @Nullable MeterRegistry meterRegistry,
      @Nonnull Duration summaryInterval,
      int maxTrackedKeys,
      @Nonnull LongSupplier clockMillis,
      @Nonnull Consumer<String> summarySink) {
    this.meterRegistry = meterRegistry;
    this.summaryIntervalMillis = Math.max(1L, summaryInterval.toMillis());
    this.maxTrackedKeys = Math.max(0, maxTrackedKeys);
    this.clockMillis = clockMillis;
    this.summarySink = summarySink;
    this.windowStartMillis = new AtomicLong(clockMillis.getAsLong());
  }

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

    maybeWriteSummary();
  }

  private void maybeWriteSummary() {
    long now = clockMillis.getAsLong();
    long start = windowStartMillis.get();
    // Only the thread that wins the swap writes the summary; others carry on.
    if (now - start < summaryIntervalMillis || !windowStartMillis.compareAndSet(start, now)) {
      return;
    }
    long windowSeconds = (now - start) / 1000;
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
