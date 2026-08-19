package com.linkedin.metadata.config.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class PostgresSqlSetupPropertiesPgQueueTopicsTest {

  private static final String BANDS_JSON =
      "[{\"range\":[0,3],\"weight\":70},{\"range\":[4,6],\"weight\":20},{\"range\":[7,9],\"weight\":10}]";

  @Test
  public void buildPgQueueResolvedTopicCatalog_inheritsKafkaTopicsAndAppliesPgOnlyOverrides() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().setInheritKafkaTopics(true);
    PgQueueTopicOverride uh = new PgQueueTopicOverride();
    uh.setMaxRowsPerTopic(10L);
    props.getPgQueue().getTopics().put("datahubUpgradeHistory", uh);

    KafkaConfiguration kafka = new KafkaConfiguration();
    TopicsConfiguration.TopicConfiguration td = new TopicsConfiguration.TopicConfiguration();
    td.setPartitions(1);
    kafka.setTopicDefaults(td);

    Map<String, TopicsConfiguration.TopicConfiguration> kt = new HashMap<>();
    TopicsConfiguration.TopicConfiguration duh = new TopicsConfiguration.TopicConfiguration();
    duh.setName("DataHubUpgradeHistory_v1");
    duh.setConfigProperties(Map.of("retention.ms", "-1"));
    kt.put("datahubUpgradeHistory", duh);

    TopicsConfiguration.TopicConfiguration mcl = new TopicsConfiguration.TopicConfiguration();
    mcl.setName("MetadataChangeLog_Timeseries_v1");
    mcl.setConfigProperties(Map.of("retention.ms", "7776000000"));
    kt.put("metadataChangeLogTimeseries", mcl);

    kafka.setTopics(kt);
    kafka.initializeTopicDefaults();

    props.validateForUse(DatabaseType.POSTGRES);
    PgQueueSetupOptions q = props.buildPgQueueOptions(kafka);
    assertTrue(
        q.getResolvedTopicCatalog().stream()
            .anyMatch(
                e ->
                    "DataHubUpgradeHistory_v1".equals(e.getTopicName())
                        && e.getMaxRowsPerTopic() == 10L
                        && e.getRetentionMaxAgeSeconds() == 0));
    assertTrue(
        q.getResolvedTopicCatalog().stream()
            .anyMatch(
                e ->
                    "MetadataChangeLog_Timeseries_v1".equals(e.getTopicName())
                        && e.getRetentionMaxAgeSeconds() == 7776000));
  }

  @Test
  public void buildPgQueueResolvedTopicCatalog_mergesOverridesWithDefaults() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().setInheritKafkaTopics(false);

    PgQueueTopicOverride ts = new PgQueueTopicOverride();
    ts.setTopicName("MetadataChangeLog_Timeseries_v1");
    ts.setRetentionMaxAgeSeconds(7776000);
    props.getPgQueue().getTopics().put("metadataChangeLogTimeseries", ts);

    PgQueueTopicOverride uh = new PgQueueTopicOverride();
    uh.setTopicName("DataHubUpgradeHistory_v1");
    uh.setRetentionMaxAgeSeconds(0);
    uh.setMaxRowsPerTopic(10L);
    props.getPgQueue().getTopics().put("datahubUpgradeHistory", uh);

    props.validateForUse(DatabaseType.POSTGRES);

    PgQueueSetupOptions q = props.buildPgQueueOptions();
    assertEquals(q.getResolvedTopicCatalog().size(), 2);
    assertTrue(
        q.getResolvedTopicCatalog().stream()
            .anyMatch(
                e ->
                    e.getTopicName().equals("MetadataChangeLog_Timeseries_v1")
                        && e.getRetentionMaxAgeSeconds() == 7776000
                        && e.getPartitionCount() == 2));
    assertTrue(
        q.getResolvedTopicCatalog().stream()
            .anyMatch(
                e ->
                    e.getTopicName().equals("DataHubUpgradeHistory_v1")
                        && e.getRetentionMaxAgeSeconds() == 0
                        && e.getMaxRowsPerTopic() == 10L));

    assertEquals(props.maxMergedPgQueueTopicRetentionMaxAgeSeconds(), 7776000);
  }

  @Test
  public void buildPgQueueResolvedTopicCatalog_mergesConsumerConcurrency() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().getTopicDefaults().setConsumerConcurrency(2);
    props.getPgQueue().setInheritKafkaTopics(false);

    PgQueueTopicOverride t = new PgQueueTopicOverride();
    t.setTopicName("TopicOne_v1");
    t.setConsumerConcurrency(5);
    props.getPgQueue().getTopics().put("topicOne", t);

    props.validateForUse(DatabaseType.POSTGRES);
    PgQueueSetupOptions q = props.buildPgQueueOptions();
    assertEquals(q.getTopicDefaultConsumerConcurrency(), 2);
    assertEquals(
        q.getResolvedTopicCatalog().stream()
            .filter(e -> "TopicOne_v1".equals(e.getTopicName()))
            .findFirst()
            .orElseThrow()
            .getConsumerConcurrency(),
        2);
  }

  @Test
  public void buildPgQueueOptions_normalizesTopicDefaultConsumerConcurrencyZero() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().getTopicDefaults().setConsumerConcurrency(0);
    props.validateForUse(DatabaseType.POSTGRES);
    PgQueueSetupOptions q = props.buildPgQueueOptions();
    assertEquals(q.getTopicDefaultConsumerConcurrency(), 1);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void validatePgQueue_rejectsNegativeTopicDefaultConsumerConcurrency() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().getTopicDefaults().setConsumerConcurrency(-1);
    props.validateForUse(DatabaseType.POSTGRES);
  }

  @Test
  public void buildPgQueueResolvedTopicCatalog_normalizesZeroOverrideToOne() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().getTopicDefaults().setConsumerConcurrency(3);
    props.getPgQueue().setInheritKafkaTopics(false);
    PgQueueTopicOverride t = new PgQueueTopicOverride();
    t.setTopicName("T_v1");
    t.setConsumerConcurrency(0);
    props.getPgQueue().getTopics().put("k", t);
    props.validateForUse(DatabaseType.POSTGRES);
    PgQueueSetupOptions q = props.buildPgQueueOptions();
    assertEquals(
        q.getResolvedTopicCatalog().stream()
            .filter(e -> "T_v1".equals(e.getTopicName()))
            .findFirst()
            .orElseThrow()
            .getConsumerConcurrency(),
        1);
  }

  @Test
  public void buildPgQueueResolvedTopicCatalog_aggressiveRetentionDefault() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().setInheritKafkaTopics(false);
    PgQueueTopicOverride t = new PgQueueTopicOverride();
    t.setTopicName("TopicNoAggr_v1");
    props.getPgQueue().getTopics().put("noAggr", t);
    props.validateForUse(DatabaseType.POSTGRES);
    PgQueueSetupOptions q = props.buildPgQueueOptions();
    assertFalse(q.isTopicDefaultAggressiveRetention());
    assertFalse(
        q.getResolvedTopicCatalog().stream()
            .filter(e -> "TopicNoAggr_v1".equals(e.getTopicName()))
            .findFirst()
            .orElseThrow()
            .isAggressiveRetention());
  }

  @Test
  public void buildPgQueueResolvedTopicCatalog_aggressiveRetentionPerTopicOverride() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().setInheritKafkaTopics(false);
    PgQueueTopicOverride t = new PgQueueTopicOverride();
    t.setTopicName("Aggressive_v1");
    t.setAggressiveRetention(true);
    props.getPgQueue().getTopics().put("aggressive", t);
    props.validateForUse(DatabaseType.POSTGRES);
    PgQueueSetupOptions q = props.buildPgQueueOptions();
    assertTrue(
        q.getResolvedTopicCatalog().stream()
            .filter(e -> "Aggressive_v1".equals(e.getTopicName()))
            .findFirst()
            .orElseThrow()
            .isAggressiveRetention());
  }

  @Test
  public void buildPgQueueResolvedTopicCatalog_aggressiveRetentionInheritsDefault() {
    PostgresSqlSetupProperties props = basePgQueueProps();
    props.getPgQueue().getTopicDefaults().setAggressiveRetention(true);
    props.getPgQueue().setInheritKafkaTopics(false);
    PgQueueTopicOverride t = new PgQueueTopicOverride();
    t.setTopicName("InheritAggr_v1");
    props.getPgQueue().getTopics().put("inherit", t);
    props.validateForUse(DatabaseType.POSTGRES);
    PgQueueSetupOptions q = props.buildPgQueueOptions();
    assertTrue(q.isTopicDefaultAggressiveRetention());
    assertTrue(
        q.getResolvedTopicCatalog().stream()
            .filter(e -> "InheritAggr_v1".equals(e.getTopicName()))
            .findFirst()
            .orElseThrow()
            .isAggressiveRetention());
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
    d.setPriorityBands(BANDS_JSON);
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
