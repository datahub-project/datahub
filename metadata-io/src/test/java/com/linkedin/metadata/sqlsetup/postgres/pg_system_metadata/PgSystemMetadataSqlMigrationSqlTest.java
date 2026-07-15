package com.linkedin.metadata.sqlsetup.postgres.pg_system_metadata;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlUtils;
import org.testng.annotations.Test;

public class PgSystemMetadataSqlMigrationSqlTest {

  @Test
  public void schemaSqlSubstitutesTableToken() throws Exception {
    String raw =
        PostgresSqlUtils.loadClasspathSql(
            getClass().getClassLoader(), "sqlsetup/pg_system_metadata/migrations/V001__schema.sql");

    String substituted =
        PostgresSqlUtils.applyTokenReplacements(
            raw,
            java.util.Map.of(
                PgSystemMetadataSqlMigrationTokens.TOKEN_TABLE_NAME, "system_metadata_service_v1"));

    assertFalse(substituted.contains("__PGSYSTEMMETADATA_TABLE__"));
    assertTrue(substituted.contains("system_metadata_service_v1"));
  }
}
