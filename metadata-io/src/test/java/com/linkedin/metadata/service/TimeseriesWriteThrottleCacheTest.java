package com.linkedin.metadata.service;

import static org.testng.Assert.*;

import com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration;
import org.testng.annotations.Test;

public class TimeseriesWriteThrottleCacheTest {

  private static final String ENTITY_DATASET = "dataset";
  private static final String URN_1 = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table1,PROD)";
  private static final String URN_2 = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table2,PROD)";
  private static final String ASPECT_PROFILE = "datasetProfile";
  private static final String ASPECT_USAGE = "datasetUsageStatistics";
  private static final String ASPECT_OPERATION = "operation";

  private TimeseriesWriteThrottleCache buildCache(
      boolean entityEnabled, boolean tsEnabled, long refreshSec, int maxUrns) {
    return buildCache(entityEnabled, tsEnabled, refreshSec, "{}", maxUrns);
  }

  private TimeseriesWriteThrottleCache buildCache(
      boolean entityEnabled,
      boolean tsEnabled,
      long refreshSec,
      String overridesJson,
      int maxUrns) {
    return buildCache(entityEnabled, tsEnabled, false, refreshSec, overridesJson, maxUrns);
  }

  private TimeseriesWriteThrottleCache buildCache(
      boolean entityEnabled,
      boolean tsEnabled,
      boolean observeEnabled,
      long refreshSec,
      String overridesJson,
      int maxUrns) {
    return new TimeseriesWriteThrottleCache(
        TimeseriesWriteThrottleConfiguration.builder()
            .entityIndex(
                TimeseriesWriteThrottleConfiguration.IndexThrottleConfig.builder()
                    .enabled(entityEnabled)
                    .build())
            .timeseriesIndex(
                TimeseriesWriteThrottleConfiguration.IndexThrottleConfig.builder()
                    .enabled(tsEnabled)
                    .build())
            .observe(
                TimeseriesWriteThrottleConfiguration.IndexThrottleConfig.builder()
                    .enabled(observeEnabled)
                    .build())
            .refreshPeriodSeconds(refreshSec)
            .refreshOverrides(overridesJson)
            .maxCacheUrns(maxUrns)
            .build());
  }

  @Test
  public void testFirstWriteIsNeverThrottled() {
    TimeseriesWriteThrottleCache cache = buildCache(true, true, 3600, 10_000);
    long now = System.currentTimeMillis();

    assertFalse(cache.shouldThrottle(ENTITY_DATASET, URN_1, ASPECT_PROFILE, now));
  }

  @Test
  public void testSecondWriteWithinRefreshPeriodIsThrottled() {
    TimeseriesWriteThrottleCache cache = buildCache(true, true, 3600, 10_000);
    long t0 = 1_000_000_000L;
    long t1 = t0 + 1000;

    cache.recordWrite(URN_1, ASPECT_PROFILE, t0);
    assertTrue(cache.shouldThrottle(ENTITY_DATASET, URN_1, ASPECT_PROFILE, t1));
  }

  @Test
  public void testWriteAfterRefreshPeriodPasses() {
    TimeseriesWriteThrottleCache cache = buildCache(true, true, 60, 10_000);
    long t0 = 1_000_000_000L;
    long t1 = t0 + 61_000;

    cache.recordWrite(URN_1, ASPECT_PROFILE, t0);
    assertFalse(cache.shouldThrottle(ENTITY_DATASET, URN_1, ASPECT_PROFILE, t1));
  }

  @Test
  public void testSameTimestampNotThrottled() {
    TimeseriesWriteThrottleCache cache = buildCache(true, true, 3600, 10_000);
    long t0 = 1_000_000_000L;

    cache.recordWrite(URN_1, ASPECT_PROFILE, t0);
    assertFalse(cache.shouldThrottle(ENTITY_DATASET, URN_1, ASPECT_PROFILE, t0));

    assertTrue(cache.shouldThrottle(ENTITY_DATASET, URN_1, ASPECT_PROFILE, t0 + 1));
  }

  @Test
  public void testDifferentAspectsOnSameUrnTrackedIndependently() {
    TimeseriesWriteThrottleCache cache = buildCache(true, false, 3600, 10_000);
    long t0 = 1_000_000_000L;
    long t1 = t0 + 1000;

    cache.recordWrite(URN_1, ASPECT_PROFILE, t0);

    assertTrue(cache.shouldThrottle(ENTITY_DATASET, URN_1, ASPECT_PROFILE, t1));
    assertFalse(cache.shouldThrottle(ENTITY_DATASET, URN_1, ASPECT_USAGE, t1));
  }

  @Test
  public void testDifferentUrnsTrackedIndependently() {
    TimeseriesWriteThrottleCache cache = buildCache(true, false, 3600, 10_000);
    long t0 = 1_000_000_000L;
    long t1 = t0 + 1000;

    cache.recordWrite(URN_1, ASPECT_PROFILE, t0);

    assertTrue(cache.shouldThrottle(ENTITY_DATASET, URN_1, ASPECT_PROFILE, t1));
    assertFalse(cache.shouldThrottle(ENTITY_DATASET, URN_2, ASPECT_PROFILE, t1));
  }

  @Test
  public void testDisabledCacheNeverThrottles() {
    TimeseriesWriteThrottleCache cache = buildCache(false, false, 3600, 10_000);
    long t0 = 1_000_000_000L;
    long t1 = t0 + 1000;

    cache.recordWrite(URN_1, ASPECT_PROFILE, t0);
    assertFalse(cache.shouldThrottle(ENTITY_DATASET, URN_1, ASPECT_PROFILE, t1));
  }

  @Test
  public void testEnabledFlags() {
    TimeseriesWriteThrottleCache entityOnly = buildCache(true, false, 3600, 10_000);
    assertTrue(entityOnly.isEntityIndexEnabled());
    assertFalse(entityOnly.isTimeseriesIndexEnabled());
    assertTrue(entityOnly.isEnabled());

    TimeseriesWriteThrottleCache tsOnly = buildCache(false, true, 3600, 10_000);
    assertFalse(tsOnly.isEntityIndexEnabled());
    assertTrue(tsOnly.isTimeseriesIndexEnabled());
    assertTrue(tsOnly.isEnabled());

    TimeseriesWriteThrottleCache neither = buildCache(false, false, 3600, 10_000);
    assertFalse(neither.isEnabled());
  }

  @Test
  public void testCacheEvictionByMaxSize() {
    TimeseriesWriteThrottleCache cache = buildCache(true, false, 3600, 2);
    long t0 = 1_000_000_000L;
    long t1 = t0 + 1000;

    cache.recordWrite(URN_1, ASPECT_PROFILE, t0);
    cache.recordWrite(URN_2, ASPECT_PROFILE, t0);

    String urn3 = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table3,PROD)";
    cache.recordWrite(urn3, ASPECT_PROFILE, t0);

    cache.getCache().cleanUp();

    assertFalse(cache.shouldThrottle(ENTITY_DATASET, URN_1, ASPECT_PROFILE, t1));
  }

  @Test
  public void testMultipleAspectsShareSameUrnEntry() {
    TimeseriesWriteThrottleCache cache = buildCache(true, true, 3600, 10_000);
    long t0 = 1_000_000_000L;

    cache.recordWrite(URN_1, ASPECT_PROFILE, t0);
    cache.recordWrite(URN_1, ASPECT_USAGE, t0);

    assertEquals(cache.getCache().size(), 1);
  }

  // --- Per-entity, per-aspect override tests ---

  @Test
  public void testOverrideRefreshPeriodForSpecificAspect() {
    // Default 3600s, but operation aspect on dataset gets 60s
    TimeseriesWriteThrottleCache cache =
        buildCache(true, true, 3600, "{\"dataset\": {\"operation\": 60}}", 10_000);
    long t0 = 1_000_000_000L;

    cache.recordWrite(URN_1, ASPECT_OPERATION, t0);
    cache.recordWrite(URN_1, ASPECT_PROFILE, t0);

    long t1 = t0 + 61_000; // 61s later

    // operation at 61s: past the 60s override → not throttled
    assertFalse(cache.shouldThrottle(ENTITY_DATASET, URN_1, ASPECT_OPERATION, t1));
    // datasetProfile at 61s: still within the 3600s default → throttled
    assertTrue(cache.shouldThrottle(ENTITY_DATASET, URN_1, ASPECT_PROFILE, t1));
  }

  @Test
  public void testOverrideDoesNotAffectOtherEntities() {
    TimeseriesWriteThrottleCache cache =
        buildCache(true, true, 3600, "{\"dataset\": {\"operation\": 60}}", 10_000);

    // "chart" entity has no override → falls back to default 3600s
    assertEquals(cache.getRefreshPeriodMs("chart", ASPECT_OPERATION), 3600_000L);
    // "dataset" entity + "operation" aspect → 60s override
    assertEquals(cache.getRefreshPeriodMs(ENTITY_DATASET, ASPECT_OPERATION), 60_000L);
    // "dataset" entity + other aspect → default
    assertEquals(cache.getRefreshPeriodMs(ENTITY_DATASET, ASPECT_PROFILE), 3600_000L);
  }

  @Test
  public void testInvalidOverrideJsonFallsBackToDefault() {
    TimeseriesWriteThrottleCache cache = buildCache(true, true, 3600, "not-valid-json", 10_000);

    assertEquals(cache.getRefreshPeriodMs(ENTITY_DATASET, ASPECT_OPERATION), 3600_000L);
  }

  @Test
  public void testRecordWriteUsesMaxTimestamp() {
    TimeseriesWriteThrottleCache cache = buildCache(true, true, 3600, 10_000);
    long t0 = 1_000_000_000L;
    long t1 = t0 + 5000;

    // Record newer first, then older (out-of-order) — max should win
    cache.recordWrite(URN_1, ASPECT_PROFILE, t1);
    cache.recordWrite(URN_1, ASPECT_PROFILE, t0);

    // 2s after t1 → should still be throttled (max is t1)
    assertTrue(cache.shouldThrottle(ENTITY_DATASET, URN_1, ASPECT_PROFILE, t1 + 2000));
  }

  // --- ThrottleSummary tests ---

  @Test
  public void testThrottleSummaryAccumulation() {
    TimeseriesWriteThrottleCache cache = buildCache(true, true, 3600, 10_000);
    TimeseriesWriteThrottleCache.ThrottleSummary summary = cache.newSummary();

    summary.recordWritten(TimeseriesWriteThrottleCache.ThrottleTarget.ENTITY_INDEX);
    summary.recordWritten(TimeseriesWriteThrottleCache.ThrottleTarget.ENTITY_INDEX);
    summary.recordSuppressed(TimeseriesWriteThrottleCache.ThrottleTarget.ENTITY_INDEX);
    summary.recordWritten(TimeseriesWriteThrottleCache.ThrottleTarget.TIMESERIES_INDEX);
    summary.recordSuppressed(TimeseriesWriteThrottleCache.ThrottleTarget.TIMESERIES_INDEX);
    summary.recordSuppressed(TimeseriesWriteThrottleCache.ThrottleTarget.TIMESERIES_INDEX);

    assertEquals(summary.getEntityIndexWritten(), 2);
    assertEquals(summary.getEntityIndexSuppressed(), 1);
    assertEquals(summary.getTimeseriesIndexWritten(), 1);
    assertEquals(summary.getTimeseriesIndexSuppressed(), 2);
  }

  @Test
  public void testThrottleSummaryNoLogWhenNothingSuppressed() {
    TimeseriesWriteThrottleCache cache = buildCache(true, true, 3600, 10_000);
    TimeseriesWriteThrottleCache.ThrottleSummary summary = cache.newSummary();

    summary.recordWritten(TimeseriesWriteThrottleCache.ThrottleTarget.ENTITY_INDEX);
    summary.recordWritten(TimeseriesWriteThrottleCache.ThrottleTarget.TIMESERIES_INDEX);

    assertEquals(summary.getEntityIndexSuppressed(), 0);
    assertEquals(summary.getTimeseriesIndexSuppressed(), 0);
    summary.logIfSuppressed();
  }

  // --- Observe (log-only) mode tests ---

  @Test
  public void testObserveOnlyEnabledMakesCacheActive() {
    TimeseriesWriteThrottleCache cache = buildCache(false, false, true, 3600, "{}", 10_000);

    assertTrue(cache.isEnabled());
    assertFalse(cache.isEntityIndexEnabled());
    assertFalse(cache.isTimeseriesIndexEnabled());
    assertTrue(cache.isObserveEnabled());
  }

  @Test
  public void testObserveModeShouldThrottleStillReturnsTrue() {
    // Observe mode alone enables the cache so shouldThrottle works
    TimeseriesWriteThrottleCache cache = buildCache(false, false, true, 3600, "{}", 10_000);
    long t0 = 1_000_000_000L;
    long t1 = t0 + 1000;

    cache.recordWrite(URN_1, ASPECT_PROFILE, t0);
    assertTrue(cache.shouldThrottle(ENTITY_DATASET, URN_1, ASPECT_PROFILE, t1));
  }

  @Test
  public void testObserveSummaryTracksObservedEvents() {
    TimeseriesWriteThrottleCache cache = buildCache(false, false, true, 3600, "{}", 10_000);
    TimeseriesWriteThrottleCache.ThrottleSummary summary = cache.newSummary();

    summary.recordObserved();
    summary.recordObserved();
    summary.recordObserved();

    assertEquals(summary.getObserved(), 3);
    assertEquals(summary.getEntityIndexSuppressed(), 0);
    assertEquals(summary.getTimeseriesIndexSuppressed(), 0);
    summary.logIfSuppressed();
  }

  @Test
  public void testObserveAndEntityThrottleBothEnabled() {
    TimeseriesWriteThrottleCache cache = buildCache(true, false, true, 3600, "{}", 10_000);

    assertTrue(cache.isEnabled());
    assertTrue(cache.isEntityIndexEnabled());
    assertTrue(cache.isObserveEnabled());
  }
}
