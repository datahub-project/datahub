package com.linkedin.metadata.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.RegistryFit;
import com.linkedin.metadata.models.registry.RegistryKnowledge;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Skips data this version's entity registry doesn't know, the same way at every site: rows, events,
 * requests and references naming entity types or aspects written by a newer version (read after a
 * zero-downtime upgrade rollback). Classification is {@link RegistryKnowledge}; this adds the
 * reporting.
 *
 * <p>Each site holds one guard:
 *
 * <pre>{@code
 * private static final UnknownDataGuard GUARD = UnknownDataGuard.forSite(MyConsumer.class, "MCL");
 * ...
 * if (!GUARD.admit(registry, opContext.getMetricUtils(), entityType, aspectName, urn)) {
 *   continue;
 * }
 * }</pre>
 *
 * <p>Every skip is counted in {@link #SKIPPED_METRIC} when the caller has metrics. After a rollback
 * a skip can happen for every event in a backlog or every row read, so the WARN is logged at most
 * once per minute per site, kind of data and key, with the number of skips since the last report.
 * Keys come from data, so the windows are kept in a bounded cache that forgets idle keys. Malformed
 * input is admitted, so existing validation still reports it as an error.
 */
public final class UnknownDataGuard {

  public static final String SKIPPED_METRIC = "unknown_to_registry_skipped";

  private static final long LOG_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);

  // Shared by every guard in the process. Keys come from data (entity type / aspect names), so the
  // cache is bounded and drops windows nobody has hit for a while.
  private static final Cache<String, Window> WINDOWS =
      CacheBuilder.newBuilder().maximumSize(10_000).expireAfterAccess(10, TimeUnit.MINUTES).build();

  private final Class<?> site;
  private final String what;
  private final Logger log;
  private final LongSupplier nanoClock;

  private UnknownDataGuard(
      @Nonnull final Class<?> site,
      @Nonnull final String what,
      @Nonnull final Logger log,
      @Nonnull final LongSupplier nanoClock) {
    this.site = site;
    this.what = what;
    this.log = log;
    this.nanoClock = nanoClock;
  }

  /**
   * @param site the class doing the skipping; names the metric and the logger
   * @param what what is skipped, for the log line (e.g. "MCL", "CDC row", "search hit")
   */
  @Nonnull
  public static UnknownDataGuard forSite(@Nonnull final Class<?> site, @Nonnull final String what) {
    return new UnknownDataGuard(site, what, LoggerFactory.getLogger(site), System::nanoTime);
  }

  @VisibleForTesting
  static UnknownDataGuard forSite(
      @Nonnull final Class<?> site,
      @Nonnull final String what,
      @Nonnull final Logger log,
      @Nonnull final LongSupplier nanoClock) {
    return new UnknownDataGuard(site, what, log, nanoClock);
  }

  /**
   * True when data naming this entity type and (optional) aspect can be processed. Otherwise the
   * skip is counted and logged, and the caller drops the data.
   */
  public boolean admit(
      @Nonnull final EntityRegistry registry,
      @Nonnull final Optional<MetricUtils> metricUtils,
      @Nullable final String entityType,
      @Nullable final String aspectName,
      @Nullable final Object subject) {
    return admit(
        RegistryKnowledge.classify(registry, entityType, aspectName),
        metricUtils,
        entityType,
        aspectName,
        subject);
  }

  /** As {@link #admit}, for a urn string that may be null or unparseable. */
  public boolean admitUrn(
      @Nonnull final EntityRegistry registry,
      @Nonnull final Optional<MetricUtils> metricUtils,
      @Nullable final String urn,
      @Nullable final String aspectName) {
    final RegistryFit fit = RegistryKnowledge.classifyUrn(registry, urn, aspectName);
    return admit(fit, metricUtils, RegistryKnowledge.entityTypeOf(urn), aspectName, urn);
  }

  /**
   * Reports a skip the caller detected itself, e.g. an empty {@code findAspectSpec} where the spec
   * is needed anyway.
   */
  public void skipped(
      @Nonnull final Optional<MetricUtils> metricUtils,
      @Nullable final String entityType,
      @Nullable final String aspectName,
      @Nullable final Object subject) {
    report(
        metricUtils,
        entityType + "/" + aspectName,
        aspectName == null
            ? String.format("entity type '%s' is not in the entity registry", entityType)
            : String.format("'%s/%s' is not in the entity registry", entityType, aspectName),
        subject);
  }

  /** Reports a skip with a caller-specific reason, e.g. a reference GraphQL can't represent. */
  public void skippedBecause(
      @Nonnull final Optional<MetricUtils> metricUtils,
      @Nonnull final String key,
      @Nonnull final String reason,
      @Nullable final Object subject) {
    report(metricUtils, key, reason, subject);
  }

  private boolean admit(
      @Nonnull final RegistryFit fit,
      @Nonnull final Optional<MetricUtils> metricUtils,
      @Nullable final String entityType,
      @Nullable final String aspectName,
      @Nullable final Object subject) {
    if (!fit.isUnknown()) {
      return true;
    }
    skipped(
        metricUtils,
        entityType,
        fit == RegistryFit.UNKNOWN_ENTITY_TYPE ? null : aspectName,
        subject);
    return false;
  }

  private void report(
      @Nonnull final Optional<MetricUtils> metricUtils,
      @Nonnull final String key,
      @Nonnull final String reason,
      @Nullable final Object subject) {
    metricUtils.ifPresent(m -> m.increment(site, SKIPPED_METRIC, 1));
    final long now = nanoClock.getAsLong();
    final long suppressed =
        WINDOWS
            .asMap()
            .computeIfAbsent(site.getName() + '|' + what + '|' + key, k -> new Window(now))
            .tryAcquire(now);
    if (suppressed == 0) {
      log.warn("Skipping {} {}: {}", what, subject, reason);
    } else if (suppressed > 0) {
      log.warn(
          "Skipping {} {}: {} ({} more skipped since the last report)",
          what,
          subject,
          reason,
          suppressed);
    }
  }

  @VisibleForTesting
  static void resetLogWindows() {
    WINDOWS.invalidateAll();
  }

  /** Allows one log per interval; counts the calls in between. */
  private static final class Window {
    private final AtomicLong nextLogNanos;
    private final AtomicLong suppressed = new AtomicLong();

    private Window(final long createdNanos) {
      // Due immediately, so the first skip of a key is always logged.
      this.nextLogNanos = new AtomicLong(createdNanos);
    }

    /** The skips to report if this call should log, otherwise -1. */
    private long tryAcquire(final long now) {
      final long next = nextLogNanos.get();
      // Subtraction keeps the comparison correct across nanoTime overflow.
      if (now - next >= 0 && nextLogNanos.compareAndSet(next, now + LOG_INTERVAL_NANOS)) {
        return suppressed.getAndSet(0);
      }
      suppressed.incrementAndGet();
      return -1;
    }
  }
}
