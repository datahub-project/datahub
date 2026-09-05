package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class PgQueueSchemaStepTest {

  @Test
  public void testToPgCronScheduleHourly() {
    assertEquals(PgQueueSchemaStep.toPgCronSchedule(3600), "0 */1 * * *");
  }

  @Test
  public void testToPgCronScheduleEveryTwoHours() {
    assertEquals(PgQueueSchemaStep.toPgCronSchedule(7200), "0 */2 * * *");
  }

  @Test
  public void testToPgCronScheduleMinuteGranularity() {
    String s = PgQueueSchemaStep.toPgCronSchedule(300);
    assertEquals(s, "*/5 * * * *");
  }

  @Test
  public void testToPgCronScheduleClampsBelowSixtySeconds() {
    assertEquals(PgQueueSchemaStep.toPgCronSchedule(30), "*/1 * * * *");
  }

  @Test
  public void testToPgCronScheduleDaily() {
    assertEquals(PgQueueSchemaStep.toPgCronSchedule(86400), "0 0 * * *");
    try {
      PgQueueSchemaStep.toPgCronSchedule(172800);
      throw new AssertionError("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      assertEquals(expected.getMessage().contains("multi-day"), true, expected.getMessage());
    }
  }

  @Test
  public void testToPgCronScheduleRejectsNonRepresentableInterval() {
    try {
      PgQueueSchemaStep.toPgCronSchedule(5400); // 90 minutes
      throw new AssertionError("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      assertEquals(
          expected.getMessage().contains("cannot be represented"), true, expected.getMessage());
    }
  }
}
