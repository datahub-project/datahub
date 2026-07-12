package com.linkedin.datahub.upgrade.system.entityconsistency;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.ConsistencyCheckSchedule;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.testng.annotations.Test;

public class ConsistencyCheckCadenceTest {

  private static final Clock FIXED_JULY_15 =
      Clock.fixed(Instant.parse("2026-07-15T12:00:00Z"), ZoneOffset.UTC);

  @Test
  public void testCheckUpgradeId() {
    assertEquals(
        ConsistencyCheckCadence.checkUpgradeId("orphan-index-document"),
        "entity-consistency-check-orphan-index-document");
  }

  @Test
  public void testMonthlyWindowStart() {
    long windowStart =
        ConsistencyCheckCadence.windowStartEpochMs(ConsistencyCheckSchedule.MONTHLY, FIXED_JULY_15);
    assertEquals(windowStart, Instant.parse("2026-07-01T00:00:00Z").toEpochMilli());
  }

  @Test
  public void testDailyWindowStart() {
    long windowStart =
        ConsistencyCheckCadence.windowStartEpochMs(ConsistencyCheckSchedule.DAILY, FIXED_JULY_15);
    assertEquals(windowStart, Instant.parse("2026-07-15T00:00:00Z").toEpochMilli());
  }

  @Test
  public void testWeeklyWindowStart() {
    // 2026-07-15 is a Wednesday; week starts Monday 2026-07-13
    long windowStart =
        ConsistencyCheckCadence.windowStartEpochMs(ConsistencyCheckSchedule.WEEKLY, FIXED_JULY_15);
    assertEquals(windowStart, Instant.parse("2026-07-13T00:00:00Z").toEpochMilli());
  }

  @Test
  public void testIsDueWhenNeverCompleted() {
    assertTrue(
        ConsistencyCheckCadence.isDue(
            ConsistencyCheckSchedule.MONTHLY, null, false, FIXED_JULY_15));
  }

  @Test
  public void testIsDueWhenCompletedBeforeWindow() {
    long juneCompletion = Instant.parse("2026-06-20T00:00:00Z").toEpochMilli();
    assertTrue(
        ConsistencyCheckCadence.isDue(
            ConsistencyCheckSchedule.MONTHLY, juneCompletion, false, FIXED_JULY_15));
  }

  @Test
  public void testIsNotDueWhenCompletedInCurrentWindow() {
    long julyCompletion = Instant.parse("2026-07-05T00:00:00Z").toEpochMilli();
    assertFalse(
        ConsistencyCheckCadence.isDue(
            ConsistencyCheckSchedule.MONTHLY, julyCompletion, false, FIXED_JULY_15));
  }

  @Test
  public void testForceDueOverridesCadence() {
    long julyCompletion = Instant.parse("2026-07-05T00:00:00Z").toEpochMilli();
    assertTrue(
        ConsistencyCheckCadence.isDue(
            ConsistencyCheckSchedule.MONTHLY, julyCompletion, true, FIXED_JULY_15));
  }

  @Test
  public void testEveryRunAlwaysDue() {
    long justNow = Instant.parse("2026-07-15T11:59:00Z").toEpochMilli();
    assertTrue(
        ConsistencyCheckCadence.isDue(
            ConsistencyCheckSchedule.EVERY_RUN, justNow, false, FIXED_JULY_15));
  }

  @Test
  public void testParseLastCompleted() {
    assertEquals(
        ConsistencyCheckCadence.parseLastCompleted(Map.of("lastCompletedTime", "12345")),
        Long.valueOf(12345L));
    assertEquals(ConsistencyCheckCadence.parseLastCompleted(Map.of()), null);
    assertEquals(ConsistencyCheckCadence.parseLastCompleted(null), null);
    assertEquals(
        ConsistencyCheckCadence.parseLastCompleted(Map.of("lastCompletedTime", "not-a-number")),
        null);
  }
}
