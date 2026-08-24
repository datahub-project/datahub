package com.linkedin.metadata.utils.elasticsearch.canonicalization;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.QueryCanonicalizationConfiguration;
import com.linkedin.metadata.config.search.TimeCanonicalizationConfiguration;
import com.linkedin.metadata.utils.elasticsearch.canonicalization.QueryTimeCanonicalizer.CanonicalNow;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.testng.annotations.Test;

public class QueryTimeCanonicalizerTest {

  private static final long MINUTE = 60_000L;
  private static final long HOUR = 60 * MINUTE;

  private static QueryTimeCanonicalizer canonicalizer(int bucketSize, String bucketSizeUnit) {
    return canonicalizer(bucketSize, bucketSizeUnit, "UTC", "EXPAND", null);
  }

  private static QueryTimeCanonicalizer canonicalizer(
      int bucketSize,
      String bucketSizeUnit,
      String timezone,
      String rounding,
      Long fixedNowMillis) {
    final QueryCanonicalizationConfiguration config =
        QueryCanonicalizationConfiguration.builder()
            .enabled(true)
            .time(
                TimeCanonicalizationConfiguration.builder()
                    .enabled(true)
                    .bucketSize(bucketSize)
                    .bucketSizeUnit(bucketSizeUnit)
                    .timezone(timezone)
                    .rounding(rounding)
                    .build())
            .build();
    final Clock clock =
        fixedNowMillis == null
            ? null
            : Clock.fixed(Instant.ofEpochMilli(fixedNowMillis), ZoneOffset.UTC);
    return QueryTimeCanonicalizer.fromConfig(config, null, clock);
  }

  private static QueryCanonicalizationConfiguration enabledConfig(
      int bucketSize, String bucketSizeUnit, String timezone, String rounding) {
    return QueryCanonicalizationConfiguration.builder()
        .enabled(true)
        .time(
            TimeCanonicalizationConfiguration.builder()
                .enabled(true)
                .bucketSize(bucketSize)
                .bucketSizeUnit(bucketSizeUnit)
                .timezone(timezone)
                .rounding(rounding)
                .build())
        .build();
  }

  private static long utc(String isoInstant) {
    return Instant.parse(isoInstant).toEpochMilli();
  }

  // ---------------------------------------------------------------------------------------------
  // Bucket boundaries
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testFloorAndCeilAroundBoundary() {
    final QueryTimeCanonicalizer c = canonicalizer(5, "MINUTES");

    // Everything strictly inside [19:00:00, 19:05:00) floors to the same boundary.
    for (String t :
        new String[] {
          "2026-08-16T19:00:00Z",
          "2026-08-16T19:00:01Z",
          "2026-08-16T19:01:00Z",
          "2026-08-16T19:02:30Z",
          "2026-08-16T19:04:59Z",
          "2026-08-16T19:04:59.999Z"
        }) {
      assertEquals(c.floor(utc(t)), utc("2026-08-16T19:00:00Z"), "floor mismatch for " + t);
    }

    // The instant just before the bucket belongs to the previous one.
    assertEquals(c.floor(utc("2026-08-16T18:59:59Z")), utc("2026-08-16T18:55:00Z"));
    // And 19:05:00 opens the next bucket.
    assertEquals(c.floor(utc("2026-08-16T19:05:00Z")), utc("2026-08-16T19:05:00Z"));
    assertEquals(c.floor(utc("2026-08-16T19:05:01Z")), utc("2026-08-16T19:05:00Z"));

    // Ceil returns an aligned input unchanged, otherwise the next boundary.
    assertEquals(c.ceil(utc("2026-08-16T19:00:00Z")), utc("2026-08-16T19:00:00Z"));
    assertEquals(c.ceil(utc("2026-08-16T19:00:01Z")), utc("2026-08-16T19:05:00Z"));
    assertEquals(c.ceil(utc("2026-08-16T19:04:59Z")), utc("2026-08-16T19:05:00Z"));
    assertEquals(c.ceil(utc("2026-08-16T19:05:00Z")), utc("2026-08-16T19:05:00Z"));
  }

  @Test
  public void testBucketSizes() {
    final long t = utc("2026-08-16T19:03:42.123Z");
    assertEquals(canonicalizer(1, "MINUTES").floor(t), utc("2026-08-16T19:03:00Z"));
    assertEquals(canonicalizer(5, "MINUTES").floor(t), utc("2026-08-16T19:00:00Z"));
    assertEquals(canonicalizer(10, "MINUTES").floor(t), utc("2026-08-16T19:00:00Z"));
    assertEquals(canonicalizer(15, "MINUTES").floor(t), utc("2026-08-16T19:00:00Z"));
    assertEquals(canonicalizer(30, "MINUTES").floor(t), utc("2026-08-16T19:00:00Z"));
    assertEquals(canonicalizer(1, "HOURS").floor(t), utc("2026-08-16T19:00:00Z"));

    final long t2 = utc("2026-08-16T19:47:42Z");
    assertEquals(canonicalizer(15, "MINUTES").floor(t2), utc("2026-08-16T19:45:00Z"));
    assertEquals(canonicalizer(30, "MINUTES").floor(t2), utc("2026-08-16T19:30:00Z"));
    assertEquals(canonicalizer(1, "HOURS").ceil(t2), utc("2026-08-16T20:00:00Z"));
  }

  @Test
  public void testFloorHandlesPreEpochInstants() {
    // floorMod, not %, so negative epoch millis still round downwards.
    final QueryTimeCanonicalizer c = canonicalizer(5, "MINUTES");
    assertEquals(c.floor(utc("1969-12-31T23:57:30Z")), utc("1969-12-31T23:55:00Z"));
  }

  // ---------------------------------------------------------------------------------------------
  // Rounding semantics
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testExpandKeepsWindowASuperset() {
    final long raw = utc("2026-08-16T19:03:42Z");
    final QueryTimeCanonicalizer c = canonicalizer(5, "MINUTES", "UTC", "EXPAND", raw);

    final CanonicalNow now = c.now();
    assertEquals(now.raw(), raw);
    assertEquals(now.reference(), utc("2026-08-16T19:00:00Z"));
    assertEquals(now.upperBound(), utc("2026-08-16T19:05:00Z"));
    assertTrue(now.isCanonicalized());

    // The window derived from this reference must contain the exact window.
    final long exactStart = raw - 24 * HOUR;
    final long canonicalStart = now.reference() - 24 * HOUR;
    assertTrue(canonicalStart <= exactStart, "lower bound must not move forwards");
    assertTrue(now.upperBound() >= raw, "upper bound must not move backwards");
  }

  @Test
  public void testShrinkFloorsBothEnds() {
    final long raw = utc("2026-08-16T19:03:42Z");
    final QueryTimeCanonicalizer c = canonicalizer(5, "MINUTES", "UTC", "SHRINK", raw);

    final CanonicalNow now = c.now();
    assertEquals(now.reference(), utc("2026-08-16T19:00:00Z"));
    assertEquals(now.upperBound(), utc("2026-08-16T19:00:00Z"));
    // The documented trade-off: the upper bound moves backwards, hiding recent data.
    assertTrue(now.upperBound() < raw);
  }

  @Test
  public void testBoundsAlwaysMoveOutwards() {
    final QueryTimeCanonicalizer c = canonicalizer(5, "MINUTES");
    final long t = utc("2026-08-16T19:03:42Z");

    // A lower bound floors and an upper bound ceils, so both move away from the window's interior.
    // That is what keeps the canonical window a superset regardless of which operator bounds it.
    assertEquals(c.floor(t), utc("2026-08-16T19:00:00Z"));
    assertEquals(c.ceil(t), utc("2026-08-16T19:05:00Z"));
    assertTrue(c.floor(t) <= t);
    assertTrue(c.ceil(t) >= t);
  }

  // ---------------------------------------------------------------------------------------------
  // Determinism / idempotency
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testRequestsWithinOneBucketProduceIdenticalBounds() {
    final long[] arrivals = {
      utc("2026-08-16T19:00:01Z"),
      utc("2026-08-16T19:00:30Z"),
      utc("2026-08-16T19:01:10Z"),
      utc("2026-08-16T19:02:45Z"),
      utc("2026-08-16T19:04:59Z")
    };

    Long sharedReference = null;
    Long sharedUpper = null;
    for (long arrival : arrivals) {
      final CanonicalNow now = canonicalizer(5, "MINUTES", "UTC", "EXPAND", arrival).now();
      if (sharedReference == null) {
        sharedReference = now.reference();
        sharedUpper = now.upperBound();
      }
      assertEquals(now.reference(), (long) sharedReference);
      assertEquals(now.upperBound(), (long) sharedUpper);
    }
    assertEquals(sharedReference, (Long) utc("2026-08-16T19:00:00Z"));

    // The next bucket must differ, otherwise the window would never advance.
    final CanonicalNow next =
        canonicalizer(5, "MINUTES", "UTC", "EXPAND", utc("2026-08-16T19:05:01Z")).now();
    assertNotEquals(next.reference(), (long) sharedReference);
    assertEquals(next.reference(), utc("2026-08-16T19:05:00Z"));
  }

  @Test
  public void testIdempotency() {
    final QueryTimeCanonicalizer c = canonicalizer(15, "MINUTES");
    for (long t :
        new long[] {
          utc("2026-08-16T19:03:42.123Z"),
          utc("2026-08-16T19:00:00Z"),
          utc("2026-08-16T19:14:59.999Z"),
          utc("2026-08-16T00:00:00Z")
        }) {
      assertEquals(c.floor(c.floor(t)), c.floor(t));
      assertEquals(c.ceil(c.ceil(t)), c.ceil(t));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Disabled behavior
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testDisabledIsPassThrough() {
    final long t = utc("2026-08-16T19:03:42.123Z");
    final QueryTimeCanonicalizer c = QueryTimeCanonicalizer.DISABLED;

    assertFalse(c.isEnabled());
    assertEquals(c.floor(t), t);
    assertEquals(c.ceil(t), t);

    final CanonicalNow now = c.now();
    assertFalse(now.isCanonicalized());
    assertEquals(now.reference(), now.raw());
    assertEquals(now.upperBound(), now.raw());
  }

  @Test
  public void testConfigSwitchesProduceDisabledInstance() {
    assertFalse(QueryTimeCanonicalizer.fromConfig(null, null).isEnabled());

    assertFalse(
        QueryTimeCanonicalizer.fromConfig(
                QueryCanonicalizationConfiguration.builder()
                    .enabled(false)
                    .time(
                        TimeCanonicalizationConfiguration.builder()
                            .enabled(true)
                            .bucketSize(5)
                            .bucketSizeUnit("MINUTES")
                            .build())
                    .build(),
                null)
            .isEnabled(),
        "the master switch must override the time strategy");

    assertFalse(
        QueryTimeCanonicalizer.fromConfig(
                QueryCanonicalizationConfiguration.builder()
                    .enabled(true)
                    .time(TimeCanonicalizationConfiguration.builder().enabled(false).build())
                    .build(),
                null)
            .isEnabled());
  }

  // ---------------------------------------------------------------------------------------------
  // Configuration validation
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testBucketSizeUnitIsHonored() {
    assertEquals(canonicalizer(500, "MILLISECONDS").getBucketMillis(), 500L);
    assertEquals(canonicalizer(30, "SECONDS").getBucketMillis(), 30_000L);
    assertEquals(canonicalizer(5, "MINUTES").getBucketMillis(), 5 * MINUTE);
    assertEquals(canonicalizer(1, "HOURS").getBucketMillis(), HOUR);
    assertEquals(canonicalizer(1, "DAYS").getBucketMillis(), 24 * HOUR);
    // Omitting the unit means SECONDS, per ParseUtils.
    assertEquals(canonicalizer(30, null).getBucketMillis(), 30_000L);
    // A sub-millisecond unit is fine as long as it lands on a whole millisecond: the restriction is
    // on truncation, not on the unit itself.
    assertEquals(canonicalizer(2_000_000, "NANOSECONDS").getBucketMillis(), 2L);
  }

  @Test
  public void testInvalidConfigurationDisablesRatherThanFailing() {
    // A typo in an optional cache optimization must cost the optimization, not the service. Each of
    // these would previously have thrown out of the Spring bean method and prevented GMS starting.
    // MAX_VALUE matters most: an absurd bucket must land out of range rather than wrap into a
    // plausible small one.
    for (Object[] bad :
        new Object[][] {
          {5, "FORTNIGHTS", "UTC", "EXPAND"},
          {0, "MINUTES", "UTC", "EXPAND"},
          {-5, "MINUTES", "UTC", "EXPAND"},
          {2, "DAYS", "UTC", "EXPAND"},
          {Integer.MAX_VALUE, "DAYS", "UTC", "EXPAND"},
          {5, "MINUTES", "Mars/Olympus", "EXPAND"},
          {5, "MINUTES", "UTC", "SIDEWAYS"},
          // Sub-millisecond buckets would truncate against a millisecond-precision clock: 1500us
          // would become 1ms, and 1ns would become 0. Both must disable rather than round.
          {1500, "MICROSECONDS", "UTC", "EXPAND"},
          {1, "NANOSECONDS", "UTC", "EXPAND"},
        }) {
      final QueryTimeCanonicalizer c =
          canonicalizer((int) bad[0], (String) bad[1], (String) bad[2], (String) bad[3], null);
      assertFalse(
          c.isEnabled(),
          "expected disabled fallback for bucketSize="
              + bad[0]
              + " "
              + bad[1]
              + " tz="
              + bad[2]
              + " rounding="
              + bad[3]);
      // And it must still behave as an exact pass-through.
      final long t = utc("2026-08-16T19:03:42.123Z");
      assertEquals(c.floor(t), t);
      assertEquals(c.ceil(t), t);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Timezone / DST
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testNonUtcZoneAnchorsOnLocalMidnight() {
    // Kathmandu is UTC+5:45, so with 30m buckets local-midnight anchoring lands on :15 and :45 UTC
    // while epoch anchoring would land on :00 and :30. This is the case that distinguishes them.
    final QueryTimeCanonicalizer c = canonicalizer(30, "MINUTES", "Asia/Kathmandu", "EXPAND", null);
    final long t = utc("2026-08-16T19:03:42Z"); // 00:48:42 local, next day

    assertEquals(c.floor(t), utc("2026-08-16T18:45:00Z")); // 00:30 local
    assertEquals(c.ceil(t), utc("2026-08-16T19:15:00Z")); // 01:00 local

    // Epoch anchoring would have produced a different boundary.
    assertNotEquals(c.floor(t), t - Math.floorMod(t, 30 * MINUTE));

    assertEquals(c.floor(c.floor(t)), c.floor(t));
    assertEquals(c.ceil(c.ceil(t)), c.ceil(t));
  }

  @Test
  public void testKolkataOffsetZone() {
    // Kolkata is UTC+5:30; with 30m buckets local-midnight and epoch anchoring coincide.
    final QueryTimeCanonicalizer c = canonicalizer(30, "MINUTES", "Asia/Kolkata", "EXPAND", null);
    final long t = utc("2026-08-16T19:03:42Z"); // 00:33:42 local, next day
    assertEquals(c.floor(t), utc("2026-08-16T19:00:00Z")); // 00:30 local
    assertEquals(c.ceil(t), utc("2026-08-16T19:30:00Z")); // 01:00 local
  }

  @Test
  public void testDstSpringForwardIsIdempotentAndMonotonic() {
    // US spring-forward 2026: 2026-03-08, local 02:00 -> 03:00 (07:00 UTC).
    final QueryTimeCanonicalizer c =
        canonicalizer(15, "MINUTES", "America/New_York", "EXPAND", null);
    final long[] samples = {
      utc("2026-03-08T06:44:00Z"),
      utc("2026-03-08T06:59:59Z"),
      utc("2026-03-08T07:00:00Z"),
      utc("2026-03-08T07:00:01Z"),
      utc("2026-03-08T08:07:00Z")
    };
    long previousFloor = Long.MIN_VALUE;
    for (long t : samples) {
      final long floored = c.floor(t);
      assertTrue(floored <= t, "floor must not move forwards");
      assertTrue(floored >= previousFloor, "floor must be monotonic across the transition");
      assertEquals(c.floor(floored), floored, "floor must be idempotent across the transition");
      assertEquals(c.ceil(c.ceil(t)), c.ceil(t), "ceil must be idempotent across the transition");
      assertTrue(c.ceil(t) >= t, "ceil must not move backwards");
      previousFloor = floored;
    }
  }

  @Test
  public void testDstFallBackIsIdempotentAndMonotonic() {
    // US fall-back 2026: 2026-11-01, local 02:00 -> 01:00 (06:00 UTC); the local day is 25h long.
    final QueryTimeCanonicalizer c = canonicalizer(1, "HOURS", "America/New_York", "EXPAND", null);
    final long[] samples = {
      utc("2026-11-01T04:30:00Z"),
      utc("2026-11-01T05:30:00Z"),
      utc("2026-11-01T06:00:00Z"),
      utc("2026-11-01T06:30:00Z"),
      utc("2026-11-02T03:59:00Z")
    };
    long previousFloor = Long.MIN_VALUE;
    for (long t : samples) {
      final long floored = c.floor(t);
      assertTrue(floored <= t);
      assertTrue(floored >= previousFloor, "floor must be monotonic across the transition");
      assertEquals(c.floor(floored), floored);
      assertEquals(c.ceil(c.ceil(t)), c.ceil(t));
      previousFloor = floored;
    }
  }

  @Test
  public void testNonDividingBucketFallsBackToEpochAnchoring() {
    // 7m does not tile an hour, so local-midnight anchoring cannot be proven idempotent and the
    // canonicalizer falls back to UTC/epoch anchoring rather than risking it.
    final QueryTimeCanonicalizer c =
        canonicalizer(7, "MINUTES", "America/New_York", "EXPAND", null);
    final long t = utc("2026-08-16T19:03:42Z");
    assertEquals(c.floor(t), t - Math.floorMod(t, 7 * MINUTE));
    assertEquals(c.floor(c.floor(t)), c.floor(t));
  }

  @Test
  public void testSubHourDstShiftFallsBackToEpochAnchoring() {
    // Lord Howe shifts 30m, so its transition day is 23.5h. A 20m bucket tiles an hour but not that
    // day, leaving a 10m stub before local midnight where ceil stops being idempotent.
    final QueryTimeCanonicalizer c =
        canonicalizer(20, "MINUTES", "Australia/Lord_Howe", "EXPAND", null);

    // Standard time (+10:30), where the two anchorings disagree: epoch gives 13:00Z, local midnight
    // would give 12:50Z. A DST-time sample proves nothing - at +11:00 they coincide.
    final long t = utc("2026-06-01T13:03:42Z");
    assertEquals(c.floor(t), utc("2026-06-01T13:00:00Z"), "expected epoch anchoring");
    assertEquals(c.floor(t), t - Math.floorMod(t, 20 * MINUTE));

    // 30m divides both the hour and the shift, so local-midnight anchoring is kept. Which anchoring
    // is in force is unobservable here (every Lord Howe offset is a whole number of 30m), so assert
    // what matters: rounding stays idempotent across the real transition.
    final QueryTimeCanonicalizer local =
        canonicalizer(30, "MINUTES", "Australia/Lord_Howe", "EXPAND", null);
    for (long sample :
        new long[] {
          utc("2026-04-04T13:00:00Z"),
          utc("2026-04-04T14:59:59Z"),
          utc("2026-04-04T15:00:00Z"),
          utc("2026-04-05T02:07:00Z")
        }) {
      assertEquals(local.floor(local.floor(sample)), local.floor(sample));
      assertEquals(local.ceil(local.ceil(sample)), local.ceil(sample));
    }
  }

  @Test
  public void testUtcOffsetZonesAreTreatedAsEpochAnchored() {
    final QueryTimeCanonicalizer c = canonicalizer(5, "MINUTES", "Etc/UTC", "EXPAND", null);
    final long t = utc("2026-08-16T19:03:42Z");
    assertEquals(c.floor(t), utc("2026-08-16T19:00:00Z"));
    assertEquals(c.getZone(), ZoneId.of("Etc/UTC"));
  }

  // ---------------------------------------------------------------------------------------------
  // Configuration defaults and observability
  // ---------------------------------------------------------------------------------------------

  /**
   * Operators commonly set only {@code enabled} and {@code bucketSize}. Leaving timezone and
   * rounding unset must fall back to UTC/EXPAND rather than failing or silently picking SHRINK.
   */
  @Test
  public void testOmittedTimezoneAndRoundingUseDefaults() {
    for (String[] omitted : new String[][] {{null, null}, {"", ""}, {"  ", "  "}}) {
      final QueryTimeCanonicalizer c = canonicalizer(5, "MINUTES", omitted[0], omitted[1], null);
      assertTrue(c.isEnabled(), "omitting optional settings must not disable the feature");
      assertEquals(c.getZone(), ZoneOffset.UTC);
      assertEquals(c.getRoundingMode(), TimeRoundingMode.EXPAND);

      // EXPAND semantics really are in force, not just the enum value.
      final long t = utc("2026-08-16T19:03:42Z");
      assertEquals(c.floor(t), utc("2026-08-16T19:00:00Z"));
      assertEquals(c.ceil(t), utc("2026-08-16T19:05:00Z"));
    }
  }

  /**
   * The applied counter is the primary signal that canonicalization is running in a deployment, so
   * assert it actually fires with the expected name and tags.
   *
   * <p>{@code MetricUtils} keys its counters in a process-wide static cache, so the first test to
   * emit a given (name, tags) combination binds it to that test's registry and every later test
   * that emits the same combination increments the wrong registry. Each metric test here therefore
   * uses a tag set no other test emits.
   */
  @Test
  public void testAppliedMetricIsEmittedPerCanonicalization() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    final MetricUtils metricUtils = MetricUtils.builder().registry(registry).build();
    final QueryTimeCanonicalizer c =
        QueryTimeCanonicalizer.fromConfig(
            enabledConfig(5, "MINUTES", "UTC", "EXPAND"),
            metricUtils,
            Clock.fixed(Instant.parse("2026-08-16T19:03:42Z"), ZoneOffset.UTC));

    c.now();
    c.now();
    c.now();

    final Counter counter =
        registry.find("datahub.search.canonicalization.applied").tag("strategy", "time").counter();
    assertNotNull(counter, "applied counter was never registered");
    assertEquals(counter.count(), 3.0, "one increment per canonicalization");
    assertEquals(counter.getId().getTag("bucket_size"), "300000ms");
  }

  /**
   * {@code changed} must count only the calls where a bound actually moved, otherwise it carries no
   * information beyond {@code applied}. A request landing exactly on a bucket boundary is the one
   * case where an enabled canonicalizer is a no-op.
   *
   * <p>Uses a 10m bucket rather than 5m purely so its counters do not collide with {@link
   * #testAppliedMetricIsEmittedPerCanonicalization} - see the note on that test.
   */
  @Test
  public void testChangedMetricCountsOnlyRealMovement() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    final MetricUtils metricUtils = MetricUtils.builder().registry(registry).build();

    QueryTimeCanonicalizer.fromConfig(
            enabledConfig(10, "MINUTES", "UTC", "EXPAND"),
            metricUtils,
            Clock.fixed(Instant.parse("2026-08-16T19:03:42Z"), ZoneOffset.UTC))
        .now();
    QueryTimeCanonicalizer.fromConfig(
            enabledConfig(10, "MINUTES", "UTC", "EXPAND"),
            metricUtils,
            Clock.fixed(Instant.parse("2026-08-16T19:10:00Z"), ZoneOffset.UTC))
        .now();

    final Counter applied =
        registry
            .find("datahub.search.canonicalization.applied")
            .tag("bucket_size", "600000ms")
            .counter();
    final Counter changed =
        registry
            .find("datahub.search.canonicalization.changed")
            .tag("bucket_size", "600000ms")
            .counter();
    assertNotNull(applied);
    assertNotNull(changed, "changed counter was never registered");
    assertEquals(applied.count(), 2.0);
    assertEquals(changed.count(), 1.0, "the on-boundary call must not count as changed");
    assertEquals(changed.getId().getTag("strategy"), "time");
  }

  /**
   * A canonicalizer disabled by configuration reports skips rather than staying silent. Without
   * this an operator cannot distinguish "the feature never runs" from "the feature runs and does
   * not help" when the cache hit rate fails to move.
   */
  @Test
  public void testDisabledReportsSkipReason() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    final MetricUtils metricUtils = MetricUtils.builder().registry(registry).build();
    final QueryTimeCanonicalizer c =
        QueryTimeCanonicalizer.fromConfig(
            QueryCanonicalizationConfiguration.builder().enabled(false).build(), metricUtils);

    final CanonicalNow now = c.now();

    assertFalse(now.isCanonicalized());
    assertEquals(now.reference(), now.raw());
    assertEquals(now.upperBound(), now.raw());
    assertNull(
        registry.find("datahub.search.canonicalization.applied").counter(),
        "a disabled canonicalizer must not report work it did not do");
    assertNull(registry.find("datahub.search.canonicalization.changed").counter());
    final Counter skipped =
        registry
            .find("datahub.search.canonicalization.skipped")
            .tag("reason", "disabled")
            .counter();
    assertNotNull(skipped, "skipped counter was never registered");
    assertEquals(skipped.count(), 1.0);
  }

  /** A misconfiguration must remain visible after the startup log has rolled away. */
  @Test
  public void testInvalidConfigReportsSkipReason() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    final MetricUtils metricUtils = MetricUtils.builder().registry(registry).build();
    final QueryTimeCanonicalizer c =
        QueryTimeCanonicalizer.fromConfig(
            enabledConfig(5, "FORTNIGHTS", "UTC", "EXPAND"), metricUtils);

    c.now();

    assertFalse(c.isEnabled());
    assertNotNull(
        registry
            .find("datahub.search.canonicalization.skipped")
            .tag("reason", "invalid_config")
            .counter());
  }

  /** The shared pass-through singleton has nothing to report to and must not blow up. */
  @Test
  public void testUnconfiguredSingletonIsSilentPassThrough() {
    final CanonicalNow now = QueryTimeCanonicalizer.DISABLED.now();
    assertFalse(now.isCanonicalized());
    assertEquals(now.reference(), now.raw());
    assertEquals(now.upperBound(), now.raw());
  }
}
