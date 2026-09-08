package com.linkedin.metadata.sqlsetup.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import org.testng.annotations.Test;

public class PostgresPartmanSqlSetupSupportTest {

  @Test
  public void parentTableLiteral_escapesQuotesInSchemaAndSuffix() {
    String literal =
        PostgresPartmanSqlSetupSupport.parentTableLiteral("pub'lic", "metadata_ts_'drop");
    assertEquals(literal, "pub''lic.metadata_ts_''drop");
    String sql =
        PostgresPartmanSqlSetupSupport.partmanRetentionClearSql("partman", "pub'lic", "t_'x");
    assertTrue(sql.contains("WHERE parent_table = 'pub''lic.t_''x'"));
    assertFalse(sql.contains("parent_table = 'pub'lic"));
  }

  @Test
  public void toPgCronSchedule_rejectsMultiDayCadence() {
    expectThrows(
        IllegalArgumentException.class,
        () -> PostgresPartmanSqlSetupSupport.toPgCronSchedule(172800));
    assertEquals(PostgresPartmanSqlSetupSupport.toPgCronSchedule(86400), "0 0 * * *");
  }
}
