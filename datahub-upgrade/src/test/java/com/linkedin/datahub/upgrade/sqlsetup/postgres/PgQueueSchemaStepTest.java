package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.sqlsetup.postgres.PostgresPartmanSqlSetupSupport;
import org.testng.annotations.Test;

public class PgQueueSchemaStepTest {

  @Test
  public void testToPgCronScheduleHourly() {
    assertEquals(PostgresPartmanSqlSetupSupport.toPgCronSchedule(3600), "0 */1 * * *");
  }

  @Test
  public void testToPgCronScheduleEveryTwoHours() {
    assertEquals(PostgresPartmanSqlSetupSupport.toPgCronSchedule(7200), "0 */2 * * *");
  }

  @Test
  public void testToPgCronScheduleMinuteGranularity() {
    String s = PostgresPartmanSqlSetupSupport.toPgCronSchedule(300);
    assertEquals(s, "*/5 * * * *");
  }

  @Test
  public void testToPgCronScheduleClampsBelowSixtySeconds() {
    assertEquals(PostgresPartmanSqlSetupSupport.toPgCronSchedule(30), "*/1 * * * *");
  }

  @Test
  public void testToPgCronScheduleDaily() {
    assertEquals(PostgresPartmanSqlSetupSupport.toPgCronSchedule(86400), "0 0 * * *");
    assertEquals(PostgresPartmanSqlSetupSupport.toPgCronSchedule(172800), "0 0 */2 * *");
  }
}
