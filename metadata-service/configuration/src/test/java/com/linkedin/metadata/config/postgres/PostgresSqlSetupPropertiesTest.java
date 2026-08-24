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

  @Test
  public void buildPgTimeseriesOptions_defaultsForceOverwriteFalse() {
    PostgresSqlSetupProperties props = basePgTimeseriesProps();
    props.validateForUse(DatabaseType.POSTGRES);
    PgTimeseriesSetupOptions o = props.buildPgTimeseriesOptions();
    assertEquals(o.getDefaultStoreName(), "default");
    assertEquals(o.getDefaultStore().isForceOverwritePartmanConfig(), false);
    assertEquals(o.getDefaultStore().getRetentionMaxAgeSeconds(), 7776000);
    assertEquals(o.resolveStore("dataset", "datasetProfile").getName(), "default");
  }

  @Test
  public void buildPgTimeseriesOptions_forceOverwritePartmanConfig() {
    PostgresSqlSetupProperties props = basePgTimeseriesProps();
    props.getPgTimeseries().getPartitioning().setForceOverwritePartmanConfig(true);
    props.validateForUse(DatabaseType.POSTGRES);
    assertEquals(
        props.buildPgTimeseriesOptions().getDefaultStore().isForceOverwritePartmanConfig(), true);
  }

  @Test
  public void buildPgTimeseriesOptions_routesNamedStore() {
    PostgresSqlSetupProperties props = basePgTimeseriesProps();
    PostgresSqlSetupProperties.PgTimeseries.StoreConfig longStore =
        new PostgresSqlSetupProperties.PgTimeseries.StoreConfig();
    longStore.setTablePrefix("metadata_timeseries_long");
    PostgresSqlSetupProperties.PgTimeseries.Partitioning partitioning =
        new PostgresSqlSetupProperties.PgTimeseries.Partitioning();
    partitioning.setPartmanPartitionInterval("1 month");
    partitioning.setPartmanPremake(4);
    longStore.setPartitioning(partitioning);
    PostgresSqlSetupProperties.PgTimeseries.Retention retention =
        new PostgresSqlSetupProperties.PgTimeseries.Retention();
    retention.setMaxAgeSeconds(46656000);
    longStore.setRetention(retention);
    props.getPgTimeseries().getStores().put("long", longStore);
    props.getPgTimeseries().getRouting().put("dataset.datasetprofile", "long");
    props.validateForUse(DatabaseType.POSTGRES);

    PgTimeseriesSetupOptions o = props.buildPgTimeseriesOptions();
    assertEquals(o.resolveStore("dataset", "datasetProfile").getName(), "long");
    assertEquals(
        o.resolveStore("dataset", "datasetProfile").getTablePrefix(), "metadata_timeseries_long");
    assertEquals(o.resolveStore("dataset", "operation").getName(), "default");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void validatePgTimeseries_rejectsUnknownRouteTarget() {
    PostgresSqlSetupProperties props = basePgTimeseriesProps();
    props.getPgTimeseries().getRouting().put("dataset.datasetprofile", "missing");
    props.validateForUse(DatabaseType.POSTGRES);
  }

  @Test
  public void normalizeRoutingKey_trimsSegmentsAroundDot() {
    assertEquals(
        PostgresSqlSetupProperties.normalizeRoutingKey(" dataset. datasetprofile "),
        "dataset.datasetprofile");
  }

  @Test
  public void buildPgTimeseriesOptions_normalizesRoutingWhitespace() {
    PostgresSqlSetupProperties props = basePgTimeseriesProps();
    PostgresSqlSetupProperties.PgTimeseries.StoreConfig longStore =
        new PostgresSqlSetupProperties.PgTimeseries.StoreConfig();
    longStore.setTablePrefix("metadata_timeseries_long");
    PostgresSqlSetupProperties.PgTimeseries.Partitioning partitioning =
        new PostgresSqlSetupProperties.PgTimeseries.Partitioning();
    partitioning.setPartmanPartitionInterval("1 month");
    partitioning.setPartmanPremake(4);
    longStore.setPartitioning(partitioning);
    props.getPgTimeseries().getStores().put("long", longStore);
    props.getPgTimeseries().getRouting().put(" dataset. datasetprofile ", "long");
    props.validateForUse(DatabaseType.POSTGRES);
    assertEquals(
        props.buildPgTimeseriesOptions().resolveStore("dataset", "datasetProfile").getName(),
        "long");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void validatePgQueue_rejectsUnrepresentableCronInterval() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().getMaintenance().setCronEnabled(true);
    props.getPgQueue().getMaintenance().setIntervalSeconds(5400);
    props.getPgCron().getAdmin().setJdbcUrl("jdbc:postgresql://localhost:5432/datahub");
    props.validateForUse(DatabaseType.POSTGRES);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void validatePgTimeseries_rejectsInheritedCronWithBadInterval() {
    PostgresSqlSetupProperties props = basePgTimeseriesProps();
    props.getPgTimeseries().getMaintenance().setCronEnabled(true);
    props.getPgTimeseries().getMaintenance().setIntervalSeconds(3600);
    props.getPgCron().getAdmin().setJdbcUrl("jdbc:postgresql://localhost:5432/datahub");
    PostgresSqlSetupProperties.PgTimeseries.StoreConfig named =
        new PostgresSqlSetupProperties.PgTimeseries.StoreConfig();
    named.setTablePrefix("metadata_timeseries_named");
    PostgresSqlSetupProperties.PgTimeseries.Partitioning partitioning =
        new PostgresSqlSetupProperties.PgTimeseries.Partitioning();
    partitioning.setPartmanPartitionInterval("1 day");
    partitioning.setPartmanPremake(4);
    named.setPartitioning(partitioning);
    PostgresSqlSetupProperties.PgTimeseries.Maintenance maintenance =
        new PostgresSqlSetupProperties.PgTimeseries.Maintenance();
    // cronEnabled omitted (null) → inherits true from default; bad interval must still fail.
    maintenance.setIntervalSeconds(5400);
    named.setMaintenance(maintenance);
    props.getPgTimeseries().getStores().put("named", named);
    props.validateForUse(DatabaseType.POSTGRES);
  }

  @Test
  public void namedStore_urlOnly_leavesCredentialsBlankForEbeanFallback() {
    PostgresSqlSetupProperties props = basePgTimeseriesProps();
    props.getPgTimeseries().getPool().setUsername("default_user");
    props.getPgTimeseries().getPool().setPassword("default_pass");
    PostgresSqlSetupProperties.PgTimeseries.StoreConfig named =
        new PostgresSqlSetupProperties.PgTimeseries.StoreConfig();
    named.setTablePrefix("metadata_timeseries_named");
    PostgresSqlSetupProperties.PgTimeseries.Partitioning partitioning =
        new PostgresSqlSetupProperties.PgTimeseries.Partitioning();
    partitioning.setPartmanPartitionInterval("1 day");
    partitioning.setPartmanPremake(4);
    named.setPartitioning(partitioning);
    PostgresSqlSetupProperties.PgTimeseries.Pool pool =
        new PostgresSqlSetupProperties.PgTimeseries.Pool();
    pool.setUrl("jdbc:postgresql://other-host:5432/datahub");
    named.setPool(pool);
    props.getPgTimeseries().getStores().put("named", named);

    PgTimeseriesStoreOptions built = props.buildPgTimeseriesOptions().getStores().get("named");
    assertNull(built.getPoolUsername());
    assertNull(built.getPoolPassword());
    assertEquals(built.getPoolUrl(), "jdbc:postgresql://other-host:5432/datahub");
  }

  @Test
  public void namedStore_withoutUrl_inheritsDefaultCredentials() {
    PostgresSqlSetupProperties props = basePgTimeseriesProps();
    props.getPgTimeseries().getPool().setUsername("default_user");
    props.getPgTimeseries().getPool().setPassword("default_pass");
    PostgresSqlSetupProperties.PgTimeseries.StoreConfig named =
        new PostgresSqlSetupProperties.PgTimeseries.StoreConfig();
    named.setTablePrefix("metadata_timeseries_named");
    PostgresSqlSetupProperties.PgTimeseries.Partitioning partitioning =
        new PostgresSqlSetupProperties.PgTimeseries.Partitioning();
    partitioning.setPartmanPartitionInterval("1 day");
    partitioning.setPartmanPremake(4);
    named.setPartitioning(partitioning);
    props.getPgTimeseries().getStores().put("named", named);

    PgTimeseriesStoreOptions built = props.buildPgTimeseriesOptions().getStores().get("named");
    assertEquals(built.getPoolUsername(), "default_user");
    assertEquals(built.getPoolPassword(), "default_pass");
  }

  private static PostgresSqlSetupProperties basePgTimeseriesProps() {
    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    props.setSchema("public");
    props.getPgTimeseries().setEnabled(true);
    props.getPgTimeseries().setDefaultStore("default");
    props.getPgTimeseries().setTablePrefix("metadata_timeseries");
    props.getPgTimeseries().getPartitioning().setPartmanPartitionInterval("1 day");
    props.getPgTimeseries().getPartitioning().setPartmanPremake(4);
    props.getPgTimeseries().getRetention().setMaxAgeSeconds(7776000);
    props.getPgTimeseries().getMaintenance().setCronEnabled(false);
    return props;
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
