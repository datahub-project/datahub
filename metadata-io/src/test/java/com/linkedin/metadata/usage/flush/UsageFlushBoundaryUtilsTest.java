package com.linkedin.metadata.usage.flush;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UsageFlushBoundaryUtilsTest {

  private static final ZoneId UTC = ZoneOffset.UTC;

  @Test
  public void testAlignDownHourlyUtc() {
    Instant instant = Instant.parse("2026-07-10T10:37:42.123Z");
    Instant aligned = UsageFlushBoundaryUtils.alignDown(instant, Duration.ofHours(1), UTC);
    Assert.assertEquals(aligned, Instant.parse("2026-07-10T10:00:00Z"));
  }

  @Test
  public void testAlignDownDailyUtc() {
    Instant instant = Instant.parse("2026-07-10T23:59:59Z");
    Instant aligned = UsageFlushBoundaryUtils.alignDown(instant, Duration.ofDays(1), UTC);
    Assert.assertEquals(aligned, Instant.parse("2026-07-10T00:00:00Z"));
  }

  @Test
  public void testAlignDownFifteenMinutes() {
    Instant instant = Instant.parse("2026-07-10T10:17:42Z");
    Instant aligned = UsageFlushBoundaryUtils.alignDown(instant, Duration.ofSeconds(900), UTC);
    Assert.assertEquals(aligned, Instant.parse("2026-07-10T10:15:00Z"));
  }

  @Test
  public void testNextBoundaryHourlyUtc() {
    Instant instant = Instant.parse("2026-07-10T10:37:42Z");
    Instant next = UsageFlushBoundaryUtils.nextBoundary(instant, Duration.ofHours(1), UTC);
    Assert.assertEquals(next, Instant.parse("2026-07-10T11:00:00Z"));
  }

  @Test
  public void testCrossesBoundaryFalseWithinSameHour() {
    Instant start = Instant.parse("2026-07-10T10:05:00Z");
    Instant end = Instant.parse("2026-07-10T10:59:59.999Z");
    Assert.assertFalse(
        UsageFlushBoundaryUtils.crossesBoundary(start, end, Duration.ofHours(1), UTC));
  }

  @Test
  public void testCrossesBoundaryTrueAcrossHour() {
    Instant start = Instant.parse("2026-07-10T10:50:00Z");
    Instant end = Instant.parse("2026-07-10T11:05:00Z");
    Assert.assertTrue(
        UsageFlushBoundaryUtils.crossesBoundary(start, end, Duration.ofHours(1), UTC));
  }

  @Test
  public void testBatchInvariantEndMinusOneMillisecond() {
    Instant start = Instant.parse("2026-07-10T10:00:00Z");
    Instant end = Instant.parse("2026-07-10T10:59:59.999Z");
    Instant alignedStart = UsageFlushBoundaryUtils.alignDown(start, Duration.ofHours(1), UTC);
    Instant alignedEnd =
        UsageFlushBoundaryUtils.alignDown(end.minusMillis(1), Duration.ofHours(1), UTC);
    Assert.assertEquals(alignedStart, alignedEnd);
  }
}
