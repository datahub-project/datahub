package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

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
  }

  @Test
  public void testToPgCronScheduleRejectsFiveHours() {
    expectThrows(IllegalArgumentException.class, () -> PgQueueSchemaStep.toPgCronSchedule(18000));
  }

  @Test
  public void testToPgCronScheduleRejectsSevenMinutes() {
    expectThrows(IllegalArgumentException.class, () -> PgQueueSchemaStep.toPgCronSchedule(420));
  }

  @Test
  public void testToPgCronScheduleEverySixHours() {
    assertEquals(PgQueueSchemaStep.toPgCronSchedule(21600), "0 */6 * * *");
  }

  @Test
  public void testToPgCronScheduleRejectsNinetyMinutes() {
    expectThrows(IllegalArgumentException.class, () -> PgQueueSchemaStep.toPgCronSchedule(5400));
  }

  @Test
  public void testToPgCronScheduleRejectsMultiDay() {
    expectThrows(IllegalArgumentException.class, () -> PgQueueSchemaStep.toPgCronSchedule(172800));
  }
}
