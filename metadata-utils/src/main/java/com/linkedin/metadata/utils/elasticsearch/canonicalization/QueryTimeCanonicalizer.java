package com.linkedin.metadata.utils.elasticsearch.canonicalization;

import com.linkedin.metadata.config.search.QueryCanonicalizationConfiguration;
import com.linkedin.metadata.config.search.TimeCanonicalizationConfiguration;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.zone.ZoneOffsetTransitionRule;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Canonical reference clock for query time windows.
 *
 * <p>DataHub does not use Elasticsearch date math; every query bound is an absolute epoch-milli
 * long derived from the wall clock at request time. That makes every trailing-window query unique
 * and defeats the shard request cache even for identical logical questions ("usage for this dataset
 * over the last 30 days"). This class rounds the wall clock onto a fixed bucket so that all
 * requests landing in the same bucket produce the same query.
 *
 * <p><b>Why this is applied at the point of generation.</b> Canonicalization is only safe on
 * timestamps the application derived from "now". A bound a user supplied explicitly - an API {@code
 * startTime} parameter, a lineage date picker, a point-in-time consistency bound - must be honored
 * exactly. Once a query has been assembled those are indistinguishable from each other, so rounding
 * at a central execution choke point would silently move bounds the user asked for. Callers
 * therefore apply this where they still know the value came from the clock.
 *
 * <p><b>This is not a cache TTL.</b> A 1-hour bucket does not mean results are cached for an hour.
 * It means requests within a bucket are byte-identical and therefore <i>eligible</i> to reuse a
 * shard request cache entry. Whether they actually do depends on shard routing, refreshes and cache
 * eviction, none of which this class controls.
 *
 * <p><b>Rounding.</b> Under {@link TimeRoundingMode#EXPAND} (the default) lower bounds floor and
 * upper bounds ceil, so the canonical window is always a superset of the requested window and no
 * matching document is excluded. Under {@link TimeRoundingMode#SHRINK} both ends floor, which hides
 * up to one bucket of the most recent data.
 *
 * <p><b>One clock read per call site.</b> A caller takes one {@link CanonicalNow} from {@link
 * #now()} and derives every bound from it, so both ends of a window agree. Call sites read the
 * clock separately, so they agree with each other only within a bucket.
 *
 * <p><b>Derived figures.</b> Widening is harmless for counts and sums, but a figure computed
 * <i>over</i> the window - a percent change, a per-day average, a rate - has to decide what the
 * widened edge means. See {@code GetHighlightsResolver}.
 *
 * <p>Instances are immutable and thread-safe. Reach one through {@code
 * OperationContext#getQueryTimeCanonicalizer()} rather than constructing it per call site.
 */
@Slf4j
public final class QueryTimeCanonicalizer {

  /** Configuration was valid and canonicalization is running. */
  static final String REASON_ENABLED = "enabled";

  /** The feature is switched off by configuration. */
  static final String REASON_DISABLED = "disabled";

  /** Configuration was present but unusable; see the startup error log. */
  static final String REASON_INVALID_CONFIG = "invalid_config";

  /**
   * Pass-through instance used when an {@link io.datahubproject.metadata.context.OperationContext}
   * was built without a canonicalizer at all (tests, ad-hoc contexts), and the instance {@link
   * #fromConfig} returns when the feature is off. A disabled instance does no metric work per call;
   * whether the feature is on is reported once, by {@link #METRIC_ENABLED}.
   */
  public static final QueryTimeCanonicalizer DISABLED =
      new QueryTimeCanonicalizer(false, 0L, ZoneOffset.UTC, TimeRoundingMode.EXPAND, null, null);

  private static final long ONE_HOUR_MILLIS = 3_600_000L;
  private static final long ONE_DAY_MILLIS = 86_400_000L;
  private static final int NANOS_PER_MILLI = 1_000_000;

  /**
   * Counts canonicalizations actually performed, i.e. one per {@link #now()} on an enabled
   * instance.
   */
  private static final String METRIC_APPLIED = "datahub.search.canonicalization.applied";

  /**
   * Gauge set once when the canonicalizer is built from configuration: 1 when canonicalization is
   * running, 0 when it is not, tagged with why. It lets an operator debugging "why is my cache hit
   * rate still flat" tell "the feature never runs" apart from "the feature runs and does not help"
   * without the disabled path doing any metric work per request.
   */
  static final String METRIC_ENABLED = "datahub.search.canonicalization.enabled";

  /**
   * Subset of {@link #METRIC_APPLIED} where the canonical bounds actually differ from the raw clock
   * reading. {@code applied - changed} is the number of requests that happened to land exactly on a
   * bucket boundary; a persistently large gap means the bucket is too small to be doing any work.
   */
  private static final String METRIC_CHANGED = "datahub.search.canonicalization.changed";

  @Getter private final boolean enabled;
  @Getter private final long bucketMillis;
  @Getter private final ZoneId zone;
  @Getter private final TimeRoundingMode roundingMode;

  /**
   * True when bucket boundaries are measured from the epoch rather than from local midnight. Always
   * true for UTC; also the fallback when a bucket size cannot tile a local day safely.
   */
  private final boolean epochAnchored;

  private final Clock clock;
  @Nullable private final MetricUtils metricUtils;

  QueryTimeCanonicalizer(
      boolean enabled,
      long bucketMillis,
      @Nonnull ZoneId zone,
      @Nonnull TimeRoundingMode roundingMode,
      @Nullable Clock clock,
      @Nullable MetricUtils metricUtils) {
    this.enabled = enabled;
    this.bucketMillis = bucketMillis;
    this.zone = zone;
    this.roundingMode = roundingMode;
    this.clock = clock != null ? clock : Clock.systemUTC();
    this.metricUtils = metricUtils;
    this.epochAnchored = isEpochAnchored(enabled, bucketMillis, zone);
  }

  /**
   * Builds a canonicalizer from configuration, returning a pass-through when the feature is off
   * <em>or misconfigured</em>. Either way it sets {@link #METRIC_ENABLED} once.
   *
   * <p>Anything this method can reject - an out-of-range bucket, an unknown unit, timezone or
   * rounding mode - degrades to disabled rather than failing startup: a typo should cost the
   * optimization, not the service. The error is logged and {@code
   * canonicalization.enabled{reason="invalid_config"}} reads 0, so it stays visible after the log
   * rolls away.
   *
   * <p>A non-numeric {@code bucketSize} is the exception - Spring binding rejects it before this
   * runs.
   */
  public static QueryTimeCanonicalizer fromConfig(
      @Nullable QueryCanonicalizationConfiguration config, @Nullable MetricUtils metricUtils) {
    return fromConfig(config, metricUtils, null);
  }

  /**
   * Overload accepting an explicit clock. Production code should use the two-argument form; a fixed
   * clock makes bucket-boundary behavior directly assertable in tests.
   */
  public static QueryTimeCanonicalizer fromConfig(
      @Nullable QueryCanonicalizationConfiguration config,
      @Nullable MetricUtils metricUtils,
      @Nullable Clock clock) {

    if (config == null || !config.isEnabled() || config.getTime() == null) {
      return passThrough(clock, metricUtils, REASON_DISABLED);
    }
    final TimeCanonicalizationConfiguration time = config.getTime();
    if (!time.isEnabled()) {
      return passThrough(clock, metricUtils, REASON_DISABLED);
    }

    try {
      final Duration bucketDuration = time.getBucketDuration();
      // Every bound is an epoch milli, so a sub-millisecond unit would truncate: 1500us silently
      // becomes a 1ms bucket. Reject rather than round the operator's configuration for them.
      if (bucketDuration.getNano() % NANOS_PER_MILLI != 0) {
        throw new IllegalArgumentException(
            "bucketSize must be a whole number of milliseconds, got: " + bucketDuration);
      }
      final long bucketMillis = bucketDuration.toMillis();
      if (bucketMillis <= 0) {
        throw new IllegalArgumentException("bucketSize must be positive, got: " + bucketDuration);
      }
      // A day-long bucket stretches short windows out of shape: rounding an hour usage chart to it
      // asks for a day or more of data while the chart still says "last hour".
      if (bucketMillis >= ONE_DAY_MILLIS) {
        throw new IllegalArgumentException(
            "bucketSize must be shorter than 1d, got: " + bucketDuration);
      }

      final ZoneId zone = parseZone(time.getTimezone());
      final TimeRoundingMode mode = TimeRoundingMode.fromString(time.getRounding());

      log.info(
          "Query time canonicalization enabled: bucketSize={}ms, timezone={}, rounding={}",
          bucketMillis,
          zone,
          mode);

      reportEnabled(metricUtils, true, REASON_ENABLED);
      return new QueryTimeCanonicalizer(true, bucketMillis, zone, mode, clock, metricUtils);
    } catch (RuntimeException e) {
      log.error(
          "Invalid query canonicalization configuration (bucketSize={} {}, timezone={}, "
              + "rounding={}); disabling canonicalization and continuing with exact timestamps. "
              + "Fix the configuration and restart to enable it.",
          time.getBucketSize(),
          time.getBucketSizeUnit(),
          time.getTimezone(),
          time.getRounding(),
          e);
      return passThrough(clock, metricUtils, REASON_INVALID_CONFIG);
    }
  }

  /** A disabled instance. It reports why once, here, and never per call. */
  private static QueryTimeCanonicalizer passThrough(
      @Nullable Clock clock, @Nullable MetricUtils metricUtils, @Nonnull String reason) {
    reportEnabled(metricUtils, false, reason);
    if (clock == null) {
      return DISABLED;
    }
    return new QueryTimeCanonicalizer(
        false, 0L, ZoneOffset.UTC, TimeRoundingMode.EXPAND, clock, null);
  }

  private static void reportEnabled(
      @Nullable MetricUtils metricUtils, boolean enabled, @Nonnull String reason) {
    // A mocked MetricUtils, as Spring context tests inject, has no registry. Reporting is
    // optional; it must never stop the canonicalizer, and with it GMS, from starting.
    final MeterRegistry registry = metricUtils == null ? null : metricUtils.getRegistry();
    if (registry == null) {
      return;
    }
    final double value = enabled ? 1.0 : 0.0;
    // A Supplier-backed gauge holds its value strongly; the default weak reference to a state
    // object would let the reading disappear after the next GC.
    Gauge.builder(METRIC_ENABLED, () -> value)
        .tags("strategy", "time", "reason", reason)
        .register(registry);
  }

  /**
   * Reads the clock exactly once and returns the canonical view of "now" for a single query. Every
   * bound in that query must be derived from the returned value.
   */
  @Nonnull
  public CanonicalNow now() {
    final long raw = clock.millis();
    if (!enabled) {
      return new CanonicalNow(raw, raw, raw, false);
    }
    final long reference = floor(raw);
    final long upper = roundingMode == TimeRoundingMode.EXPAND ? ceil(raw) : reference;
    if (metricUtils != null) {
      final String bucketTag = bucketMillis + "ms";
      metricUtils.incrementMicrometer(
          METRIC_APPLIED, 1, "strategy", "time", "bucket_size", bucketTag);
      // Only a request landing exactly on a bucket boundary leaves both bounds untouched.
      if (reference != raw || upper != raw) {
        metricUtils.incrementMicrometer(
            METRIC_CHANGED, 1, "strategy", "time", "bucket_size", bucketTag);
      }
    }
    return new CanonicalNow(raw, reference, upper, true);
  }

  /**
   * Largest bucket boundary at or before {@code epochMillis}. Idempotent: {@code floor(floor(t)) ==
   * floor(t)}. Identity when disabled.
   */
  public long floor(long epochMillis) {
    if (!enabled) {
      return epochMillis;
    }
    if (epochAnchored) {
      return epochMillis - Math.floorMod(epochMillis, bucketMillis);
    }
    final long anchor = localDayStartMillis(epochMillis);
    final long offset = epochMillis - anchor;
    return anchor + (offset - Math.floorMod(offset, bucketMillis));
  }

  /**
   * Smallest bucket boundary at or after {@code epochMillis}. Idempotent: an input already on a
   * boundary is returned unchanged. Identity when disabled.
   */
  public long ceil(long epochMillis) {
    if (!enabled) {
      return epochMillis;
    }
    final long floored = floor(epochMillis);
    return floored == epochMillis ? floored : floored + bucketMillis;
  }

  /**
   * Start of the local day containing {@code epochMillis}, used as the bucket anchor for non-UTC
   * zones. Anchoring on local midnight rather than the epoch keeps buckets aligned to wall-clock
   * time in zones with sub-hour offsets, and keeps rounding well defined across daylight-saving
   * transitions: all arithmetic is done on instants, never on local times, so a 23h or 25h day
   * needs no special case.
   */
  private long localDayStartMillis(long epochMillis) {
    final ZonedDateTime zdt = Instant.ofEpochMilli(epochMillis).atZone(zone);
    return zdt.toLocalDate().atStartOfDay(zone).toInstant().toEpochMilli();
  }

  private static boolean isEpochAnchored(boolean enabled, long bucketMillis, ZoneId zone) {
    if (!enabled) {
      return true;
    }
    if (ZoneOffset.UTC.equals(zone) || ZoneOffset.UTC.equals(zone.normalized())) {
      return true;
    }
    // Local-midnight anchoring needs buckets that tile a local day, including a DST-shifted one.
    // Tiling an hour is not enough: a sub-hour shift (Australia/Lord_Howe shifts 30m) leaves a
    // partial bucket at the end of the shifted day, and ceil stops being idempotent at local
    // midnight. Everything else epoch-anchors, which tiles unconditionally. Only recurring rules
    // are checked - enough for rounding the current clock.
    if (bucketMillis > ONE_HOUR_MILLIS || ONE_HOUR_MILLIS % bucketMillis != 0) {
      log.warn(
          "Query time canonicalization bucket {}ms does not evenly divide one hour; "
              + "falling back to UTC/epoch-anchored bucket boundaries for timezone {}.",
          bucketMillis,
          zone);
      return true;
    }
    for (ZoneOffsetTransitionRule rule : zone.getRules().getTransitionRules()) {
      final long shiftMillis =
          Math.abs(
                  (long) rule.getOffsetAfter().getTotalSeconds()
                      - rule.getOffsetBefore().getTotalSeconds())
              * 1000L;
      if (shiftMillis % bucketMillis != 0) {
        log.warn(
            "Query time canonicalization bucket {}ms does not evenly divide the {}ms offset shift "
                + "of timezone {}; falling back to UTC/epoch-anchored bucket boundaries.",
            bucketMillis,
            shiftMillis,
            zone);
        return true;
      }
    }
    return false;
  }

  private static ZoneId parseZone(@Nullable String timezone) {
    if (timezone == null || timezone.isBlank()) {
      return ZoneOffset.UTC;
    }
    return ZoneId.of(timezone.trim());
  }

  /**
   * The canonical view of "now" for one query: a single clock read plus the bounds derived from it.
   */
  public static final class CanonicalNow {
    private final long raw;
    private final long reference;
    private final long upperBound;
    private final boolean canonicalized;

    CanonicalNow(long raw, long reference, long upperBound, boolean canonicalized) {
      this.raw = raw;
      this.reference = reference;
      this.upperBound = upperBound;
      this.canonicalized = canonicalized;
    }

    /** The exact clock reading, for callers that need true wall time (audit stamps, logging). */
    public long raw() {
      return raw;
    }

    /**
     * The canonical anchor. Use this wherever a relative window start is computed ({@code reference
     * - 30d}) so that the start and end of one query agree.
     */
    public long reference() {
      return reference;
    }

    /** The canonical value to use where "now" is the upper bound of a window. */
    public long upperBound() {
      return upperBound;
    }

    /** False when canonicalization is disabled, i.e. all three values are the raw clock reading. */
    public boolean isCanonicalized() {
      return canonicalized;
    }
  }
}
