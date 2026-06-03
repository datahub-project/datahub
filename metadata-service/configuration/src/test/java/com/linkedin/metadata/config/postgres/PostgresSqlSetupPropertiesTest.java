package com.linkedin.metadata.config.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.Map;
import org.testng.annotations.Test;

public class PostgresSqlSetupPropertiesTest {

  @Test
  public void disabled_pgQueueNotBuilt() {
    PostgresSqlSetupProperties props = PostgresSqlSetupProperties.disabled();
    assertNull(props.buildPgQueueOptions());
    props.validateForUse(DatabaseType.MYSQL);
  }

  @Test
  public void buildPgQueueOptions_returnsNullWhenPgQueueDisabled() {
    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    props.getPgQueue().setEnabled(false);
    assertNull(props.buildPgQueueOptions());
  }

  @Test
  public void applySqlSetupSchemaFromJdbcUrl_setsPublicForPostgresWhenUnset() {
    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    props.applySqlSetupSchemaFromJdbcUrl("jdbc:postgresql://localhost:5432/datahub");
    assertEquals(props.getSchema(), "public");
  }

  @Test
  public void applySqlSetupSchemaFromJdbcUrl_leavesSchemaForMysql() {
    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    props.setSchema("custom");
    props.applySqlSetupSchemaFromJdbcUrl("jdbc:mysql://localhost:3306/datahub");
    assertEquals(props.getSchema(), "custom");
  }

  @Test
  public void retentionSecondsFromKafkaRetentionMs_parsesAndHandlesSentinels() {
    assertNull(PostgresSqlSetupProperties.retentionSecondsFromKafkaRetentionMs(null));
    assertEquals(
        PostgresSqlSetupProperties.retentionSecondsFromKafkaRetentionMs(
            Map.of("retention.ms", "86400000")),
        Integer.valueOf(86400));
    assertEquals(
        PostgresSqlSetupProperties.retentionSecondsFromKafkaRetentionMs(
            Map.of("retention.ms", "-1")),
        Integer.valueOf(0));
    assertNull(
        PostgresSqlSetupProperties.retentionSecondsFromKafkaRetentionMs(
            Map.of("retention.ms", "not-a-number")));
  }

  @Test
  public void formatPartmanRetentionIntervalText_formatsDaysHoursAndSeconds() {
    assertEquals(PostgresSqlSetupProperties.formatPartmanRetentionIntervalText(86400, 0), "1 days");
    assertEquals(PostgresSqlSetupProperties.formatPartmanRetentionIntervalText(3600, 0), "1 hours");
    assertEquals(
        PostgresSqlSetupProperties.formatPartmanRetentionIntervalText(90, 0), "90 seconds");
    assertEquals(PostgresSqlSetupProperties.formatPartmanRetentionIntervalText(0, 0), "1 day");
  }

  @Test
  public void approximatePartitionSeconds_coversAllowlistedIntervals() {
    assertEquals(PostgresSqlSetupProperties.approximatePartitionSeconds("1 hour"), 3600L);
    assertEquals(PostgresSqlSetupProperties.approximatePartitionSeconds("1 day"), 86400L);
    assertEquals(PostgresSqlSetupProperties.approximatePartitionSeconds("unknown"), 86400L);
  }

  @Test
  public void resolvePartmanPartitionRetentionIntervalText_returnsNullWhenNoRetention() {
    assertNull(
        PostgresSqlSetupProperties.resolvePartmanPartitionRetentionIntervalText(0, 0, "1 day"));
  }

  @Test
  public void normalizedPgCronSchema_defaultsToCron() {
    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    assertEquals(props.normalizedPgCronSchema(), "cron");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void validateForUse_rejectsInvalidPartitionCount() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().getTopicDefaults().setPartitionCount(0);
    props.validateForUse(DatabaseType.POSTGRES);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void validateForUse_requiresPgCronJdbcUrlWhenCronEnabled() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().getMaintenance().setCronEnabled(true);
    props.getPgCron().getAdmin().setJdbcUrl("");
    props.validateForUse(DatabaseType.POSTGRES);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void normalizeTablePrefix_rejectsInvalidIdentifier() {
    PostgresSqlSetupProperties.normalizeTablePrefix("1bad", "postgres.pgQueue.tablePrefix");
  }

  @Test
  public void resolvePartmanPartitionRetentionIntervalText_withRetention() {
    String text =
        PostgresSqlSetupProperties.resolvePartmanPartitionRetentionIntervalText(
            604800, 7776000, "1 day");
    assertEquals(text, "92 days");
  }

  @Test
  public void normalizedPostgresSchema_lowercasesValidIdentifier() {
    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    props.setSchema("MySchema");
    assertEquals(props.normalizedPostgresSchema(), "myschema");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void validateForUse_rejectsShortRetentionWhenEnabled() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().getTopicDefaults().setRetentionMaxAgeSeconds(30);
    props.validateForUse(DatabaseType.POSTGRES);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void validateForUse_rejectsInvalidPartmanInterval() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().getRetention().setPartmanPartitionInterval("2 days");
    props.validateForUse(DatabaseType.POSTGRES);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void validateForUse_rejectsInvalidPayloadCompression() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().setPayloadCompression("GZIP");
    props.validateForUse(DatabaseType.POSTGRES);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void validateForUse_rejectsBlankTopicNameWhenNotInheritingKafka() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().setInheritKafkaTopics(false);
    PgQueueTopicOverride t = new PgQueueTopicOverride();
    t.setTopicName("  ");
    props.getPgQueue().getTopics().put("bad", t);
    props.validateForUse(DatabaseType.POSTGRES);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void buildPgQueueOptions_requiresTopicNameWhenNotInheritingKafka() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().setInheritKafkaTopics(false);
    PgQueueTopicOverride t = new PgQueueTopicOverride();
    props.getPgQueue().getTopics().put("missingName", t);
    props.validateForUse(DatabaseType.POSTGRES);
    props.buildPgQueueOptions();
  }

  @Test
  public void applySqlSetupSchemaFromJdbcUrl_ignoresUnparseableUrl() {
    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    props.setSchema("keep");
    props.applySqlSetupSchemaFromJdbcUrl("not-a-jdbc-url");
    assertEquals(props.getSchema(), "keep");
  }

  @Test
  public void validateForUse_withPgCronEnabledAndJdbcUrl() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().getMaintenance().setCronEnabled(true);
    props.getPgQueue().getMaintenance().setIntervalSeconds(3600);
    props.getPgCron().getAdmin().setJdbcUrl("jdbc:postgresql://localhost:5432/datahub");
    props.validateForUse(DatabaseType.POSTGRES);
  }

  private static PostgresSqlSetupProperties basePgQueueProps() {
    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    props.setSchema("public");
    props.getPgQueue().setEnabled(true);
    props.getPgQueue().setSchema("queue");
    props.getPgQueue().setTablePrefix("metadata_queue");
    PostgresSqlSetupProperties.PgQueue.TopicDefaults d = props.getPgQueue().getTopicDefaults();
    d.setPartitionCount(2);
    d.setVisibilityTimeoutSeconds(600);
    d.setPriorityBands(
        "[{\"range\":[0,3],\"weight\":70},{\"range\":[4,6],\"weight\":20},{\"range\":[7,9],\"weight\":10}]");
    d.setRetentionMaxAgeSeconds(604800);
    d.setMaxRowsPerTopic(0L);
    d.setMaxTotalPayloadBytesPerTopic(0L);
    PostgresSqlSetupProperties.PgQueue.Retention r = props.getPgQueue().getRetention();
    r.setPartmanPartitionInterval("1 day");
    r.setPartmanPremake(4);
    props.getPgQueue().getMaintenance().setCronEnabled(false);
    props.getPgQueue().getMaintenance().setBatchDeleteLimit(5000);
    props.getPgQueue().setPayloadCompression("SNAPPY");
    return props;
  }
}
