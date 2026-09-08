package com.linkedin.metadata.analytics.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.time.Instant;
import org.testng.annotations.Test;

public class PostgresAnalyticsUtcTest {

  @Test
  public void truncateAndSeal() {
    Instant t = Instant.parse("2026-03-15T10:30:00Z");
    Instant hour = PostgresAnalyticsUtc.truncateToUtcHour(t);
    assertEquals(hour, Instant.parse("2026-03-15T10:00:00Z"));
    assertEquals(
        PostgresAnalyticsUtc.hourEndExclusive(hour), Instant.parse("2026-03-15T11:00:00Z"));
    assertEquals(PostgresAnalyticsUtc.partitionKeyHour(hour), "2026-03-15T10");

    Instant now = Instant.parse("2026-03-15T11:20:00Z");
    assertTrue(PostgresAnalyticsUtc.isHourSealable(hour, now, 900));
    assertFalse(
        PostgresAnalyticsUtc.isHourSealable(hour, Instant.parse("2026-03-15T11:10:00Z"), 900));
  }

  @Test
  public void dayAndMonthKeys() {
    Instant t = Instant.parse("2026-03-15T10:30:00Z");
    assertEquals(
        PostgresAnalyticsUtc.partitionKeyDay(PostgresAnalyticsUtc.truncateToUtcDay(t)),
        "2026-03-15");
    assertEquals(
        PostgresAnalyticsUtc.partitionKeyMonth(PostgresAnalyticsUtc.truncateToUtcMonth(t)),
        "2026-03");
  }
}
