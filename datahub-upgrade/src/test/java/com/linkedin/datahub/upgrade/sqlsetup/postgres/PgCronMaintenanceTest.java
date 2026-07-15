package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class PgCronMaintenanceTest {

  @Test
  public void testBuildScopedCronJobNameTypical() {
    String name =
        PgCronMaintenance.buildScopedCronJobName(
            PgCronMaintenance.PGQUEUE_CRON_ROLE, "datahub", "queue", "metadata_queue");
    assertEquals(name, "datahub_pgqueue_apply_retention__datahub__queue__metadata_queue");
  }

  @Test
  public void testBuildScopedCronJobNameDistinctSchema() {
    String a =
        PgCronMaintenance.buildScopedCronJobName(
            PgCronMaintenance.PGQUEUE_CRON_ROLE, "db", "tenant_a", "metadata_queue");
    String b =
        PgCronMaintenance.buildScopedCronJobName(
            PgCronMaintenance.PGQUEUE_CRON_ROLE, "db", "tenant_b", "metadata_queue");
    assertNotEquals(a, b);
  }

  @Test
  public void testBuildScopedCronJobNameStable() {
    String name1 =
        PgCronMaintenance.buildScopedCronJobName(
            PgCronMaintenance.PGTIMESERIES_CRON_ROLE, "prod", "dh", "metadata_timeseries");
    String name2 =
        PgCronMaintenance.buildScopedCronJobName(
            PgCronMaintenance.PGTIMESERIES_CRON_ROLE, "prod", "dh", "metadata_timeseries");
    assertEquals(name1, name2);
  }

  @Test
  public void testSanitizeCronSegmentReplacesInvalidCharacters() {
    assertEquals(PgCronMaintenance.sanitizeCronSegment("My-DB"), "my_db");
    assertEquals(PgCronMaintenance.sanitizeCronSegment("  DATA  "), "data");
  }

  @Test
  public void testBuildScopedCronJobNameUsesHashWhenTooLong() {
    String db = "a".repeat(30);
    String sch = "b".repeat(30);
    String pre = "c".repeat(30);
    String fullKey =
        PgCronMaintenance.PGQUEUE_CRON_ROLE
            + "__"
            + PgCronMaintenance.sanitizeCronSegment(db)
            + "__"
            + PgCronMaintenance.sanitizeCronSegment(sch)
            + "__"
            + PgCronMaintenance.sanitizeCronSegment(pre);
    assertTrue(fullKey.length() > PgCronMaintenance.MAX_SCOPED_CRON_JOB_NAME_LENGTH);
    String expected =
        PgCronMaintenance.PGQUEUE_CRON_ROLE + "__h" + PgCronMaintenance.sha256First12Hex(fullKey);
    String name =
        PgCronMaintenance.buildScopedCronJobName(PgCronMaintenance.PGQUEUE_CRON_ROLE, db, sch, pre);
    assertEquals(name, expected);
    assertTrue(name.length() <= PgCronMaintenance.MAX_SCOPED_CRON_JOB_NAME_LENGTH);
  }

  @Test
  public void testSha256First12HexDeterministic() {
    String h1 = PgCronMaintenance.sha256First12Hex("same");
    String h2 = PgCronMaintenance.sha256First12Hex("same");
    assertEquals(h1, h2);
    assertEquals(h1.length(), 12);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBuildScopedCronJobNameRejectsBlankDatabase() {
    PgCronMaintenance.buildScopedCronJobName(
        PgCronMaintenance.PGQUEUE_CRON_ROLE, "", "schema", "prefix");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBuildScopedCronJobNameRejectsBlankSchema() {
    PgCronMaintenance.buildScopedCronJobName(
        PgCronMaintenance.PGQUEUE_CRON_ROLE, "db", "", "prefix");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSanitizeCronSegmentRejectsWhitespaceOnly() {
    PgCronMaintenance.sanitizeCronSegment("   ");
  }
}
