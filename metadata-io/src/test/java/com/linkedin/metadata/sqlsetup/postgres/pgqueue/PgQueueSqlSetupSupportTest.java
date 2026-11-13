package com.linkedin.metadata.sqlsetup.postgres.pgqueue;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import org.testng.annotations.Test;

public class PgQueueSqlSetupSupportTest {

  @Test
  public void partmanRetentionUpdateSql_formatsUpdate() {
    String sql =
        PgQueueSqlSetupSupport.partmanRetentionUpdateSql(
            "partman", "queue", "7 days", "metadata_queue");
    assertTrue(sql.contains("UPDATE \"partman\".part_config"));
    assertTrue(sql.contains("queue.metadata_queue_message"));
  }

  @Test
  public void partmanRetentionUpdateSql_emptyWhenUnset() {
    assertEquals(
        PgQueueSqlSetupSupport.partmanRetentionUpdateSql("partman", "queue", null, "p"), "");
  }

  @Test
  public void buildRetentionPartmanTail_callsRunMaintenance() {
    String tail =
        PgQueueSqlSetupSupport.buildRetentionPartmanTail("partman", "queue", "metadata_queue");
    assertTrue(tail.contains("run_maintenance"));
    assertTrue(tail.contains("queue.metadata_queue_message"));
  }

  @Test
  public void sanitizePartmanIntervalLiteral_escapesQuotes() {
    assertEquals(PgQueueSqlSetupSupport.sanitizePartmanIntervalLiteral("1'day"), "1''day");
  }

  @Test
  public void pgQueueSqlMigrationModules_buildsDescriptor() {
    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    props.getPgQueue().setEnabled(true);
    props.getPgQueue().setSchema("queue");
    props.getPgQueue().setTablePrefix("metadata_queue");
    PgQueueSetupOptions options = props.buildPgQueueOptions();
    assert options != null;

    var module =
        PgQueueSqlMigrationModules.from(
            options,
            PgQueueSqlMigrationTokens.builder()
                .quotedSchema("\"queue\"")
                .tablePrefix("metadata_queue")
                .batchDeleteLimit("1000")
                .partmanParentQualified("queue.metadata_queue_message")
                .partmanInterval("1 day")
                .partmanPremake("4")
                .retentionPartmanTail("")
                .build());

    assertEquals(module.getMigrationNamespace(), PgQueueSqlMigrationModules.MIGRATION_NAMESPACE);
    assertEquals(module.getClasspathLocation(), PgQueueSqlMigrationModules.CLASSPATH_LOCATION);
    assertEquals(module.getLedgerTableName(), "metadata_queue_schema_migration");
  }
}
