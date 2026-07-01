package com.linkedin.metadata.sqlsetup.postgres.pgqueue;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlUtils;
import org.testng.annotations.Test;

public class PgQueueSqlMigrationSqlTest {

  @Test
  public void schemaSqlSubstitutesTokens() throws Exception {
    String raw =
        PostgresSqlUtils.loadClasspathSql(
            getClass().getClassLoader(), "sqlsetup/pgqueue/migrations/V001__schema.sql");

    String substituted =
        PostgresSqlUtils.applyTokenReplacements(
            raw,
            java.util.Map.of(
                PgQueueSqlMigrationTokens.TOKEN_PREFIX,
                "metadata_queue",
                PgQueueSqlMigrationTokens.TOKEN_SCHEMA,
                PostgresSqlUtils.quotePgIdentifier("queue")));

    assertFalse(substituted.contains("__PGQUEUE_PREFIX__"));
    assertFalse(substituted.contains("__PGQUEUE_SCHEMA__"));
    assertTrue(substituted.contains("CHECK (priority BETWEEN 0 AND 9)"));
  }

  @Test
  public void maintenanceSqlRetainsPartitionSequenceAnchor() throws Exception {
    String raw =
        PostgresSqlUtils.loadClasspathSql(
            getClass().getClassLoader(),
            "sqlsetup/pgqueue/migrations/R__maintenance_functions.sql");

    String substituted =
        PostgresSqlUtils.applyTokenReplacements(
            raw,
            java.util.Map.of(
                PgQueueSqlMigrationTokens.TOKEN_PREFIX,
                "metadata_queue",
                PgQueueSqlMigrationTokens.TOKEN_SCHEMA,
                PostgresSqlUtils.quotePgIdentifier("queue"),
                PgQueueSqlMigrationTokens.TOKEN_BATCH_DELETE_LIMIT,
                "5000",
                PgQueueSqlMigrationTokens.TOKEN_RETENTION_PARTMAN_TAIL,
                ""));

    assertTrue(substituted.contains("m_anchor.enqueue_seq"));
  }
}
