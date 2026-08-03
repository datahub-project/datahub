package com.linkedin.metadata.sqlsetup.postgres.pgtimeseries;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlUtils;
import org.testng.annotations.Test;

public class PgTimeseriesSqlMigrationSqlTest {

  @Test
  public void schemaSqlSubstitutesPrefixToken() throws Exception {
    String raw =
        PostgresSqlUtils.loadClasspathSql(
            getClass().getClassLoader(),
            "sqlsetup/pgtimeseries/migrations/V001__schema_partman.sql");

    String substituted =
        PostgresSqlUtils.applyTokenReplacements(
            raw,
            java.util.Map.of(PgTimeseriesSqlMigrationTokens.TOKEN_PREFIX, "metadata_timeseries"));

    assertFalse(substituted.contains("__PGTIMESERIES_PREFIX__"));
    assertTrue(substituted.contains("metadata_timeseries_aspect_row"));
  }

  @Test
  public void partmanRegisterSqlSubstitutesTokens() throws Exception {
    String raw =
        PostgresSqlUtils.loadClasspathSql(
            getClass().getClassLoader(),
            "sqlsetup/pgtimeseries/migrations/R__partman_register.sql");

    String substituted =
        PostgresSqlUtils.applyTokenReplacements(
            raw,
            java.util.Map.of(
                PgTimeseriesSqlMigrationTokens.TOKEN_PARTMAN_PARENT,
                "public.metadata_timeseries_aspect_row",
                PgTimeseriesSqlMigrationTokens.TOKEN_PARTMAN_INTERVAL,
                "1 day",
                PgTimeseriesSqlMigrationTokens.TOKEN_PARTMAN_PREMAKE,
                "4"));

    assertFalse(substituted.contains("__PARTMAN_PARENT_QUALIFIED__"));
    assertTrue(substituted.contains("create_parent"));
  }
}
