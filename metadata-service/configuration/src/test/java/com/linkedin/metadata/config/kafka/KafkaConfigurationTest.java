package com.linkedin.metadata.config.kafka;

import static org.testng.Assert.*;

import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.config.DataHubTestApplication;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(classes = DataHubTestApplication.class)
public class KafkaConfigurationTest extends AbstractTestNGSpringContextTests {

  @Autowired private DataHubTestApplication testApplication;

  @Test
  public void testKafkaConfigurationLoaded() {
    assertNotNull(testApplication, "Test application should be loaded");

    DataHubAppConfiguration appConfig = testApplication.getDataHubAppConfig();
    assertNotNull(appConfig, "DataHubAppConfiguration should be loaded");

    KafkaConfiguration kafkaConfig = appConfig.getKafka();
    assertNotNull(kafkaConfig, "KafkaConfiguration should be loaded");
  }

  @Test
  public void testConfigurationHierarchy() {
    DataHubAppConfiguration appConfig = testApplication.getDataHubAppConfig();
    KafkaConfiguration kafkaConfig = appConfig.getKafka();

    // Test that all configuration objects are properly nested
    assertNotNull(kafkaConfig.getSetup(), "Setup configuration should be accessible");
    assertNotNull(
        kafkaConfig.getTopicDefaults(), "TopicDefaults configuration should be accessible");

    // Test that the configuration objects are of the correct types
    assertTrue(
        kafkaConfig.getSetup() instanceof SetupConfiguration,
        "Setup should be SetupConfiguration type");
    assertTrue(
        kafkaConfig.getTopicDefaults() instanceof TopicsConfiguration.TopicConfiguration,
        "TopicDefaults should be TopicConfiguration type");

    // Topics configuration should not be null
    assertNotNull(kafkaConfig.getTopics(), "Topics configuration should not be null");
    assertTrue(
        kafkaConfig.getTopics() instanceof TopicsConfiguration,
        "Topics should be TopicsConfiguration type");
  }

  @Test
  public void testTopicConfigurationDefaultsInitialization() {
    DataHubAppConfiguration appConfig = testApplication.getDataHubAppConfig();
    KafkaConfiguration kafkaConfig = appConfig.getKafka();

    TopicsConfiguration topicsConfig = kafkaConfig.getTopics();
    assertNotNull(topicsConfig, "TopicsConfiguration should not be null");
    TopicsConfiguration.TopicConfiguration topicDefaults = topicsConfig.getTopicDefaults();

    // Test that topics with null values get initialized with defaults
    Map<String, TopicsConfiguration.TopicConfiguration> topics = topicsConfig.getTopics();
    assertNotNull(topics, "Topics map should not be null");

    // Test metadataChangeProposal topic (should have defaults applied)
    TopicsConfiguration.TopicConfiguration metadataChangeProposal =
        topics.get("metadataChangeProposal");
    assertNotNull(metadataChangeProposal, "metadataChangeProposal should not be null");
    // This topic only has a name, so partitions should be initialized from defaults
    assertEquals(
        metadataChangeProposal.getPartitions(),
        topicDefaults.getPartitions(),
        "metadataChangeProposal partitions should be initialized from defaults");
    assertEquals(
        metadataChangeProposal.getReplicationFactor(),
        topicDefaults.getReplicationFactor(),
        "metadataChangeProposal replicationFactor should be initialized from defaults");

    // Test configProperties merging for metadataChangeProposal (should inherit all defaults)
    Map<String, String> metadataChangeProposalConfig = metadataChangeProposal.getConfigProperties();
    assertNotNull(
        metadataChangeProposalConfig, "metadataChangeProposal configProperties should not be null");
    Map<String, String> defaultConfig = topicDefaults.getConfigProperties();
    assertNotNull(defaultConfig, "topicDefaults configProperties should not be null");

    // Verify that metadataChangeProposal inherits the default max.message.bytes
    assertEquals(
        metadataChangeProposalConfig.get("max.message.bytes"),
        defaultConfig.get("max.message.bytes"),
        "metadataChangeProposal should inherit max.message.bytes from defaults");
    assertEquals(
        metadataChangeProposalConfig.get("max.message.bytes"),
        "5242880",
        "metadataChangeProposal should have default max.message.bytes value");

    // Test datahubUpgradeHistory topic (has explicit partitions=1, should retain it)
    TopicsConfiguration.TopicConfiguration datahubUpgradeHistory =
        topics.get("datahubUpgradeHistory");
    assertNotNull(datahubUpgradeHistory, "datahubUpgradeHistory should not be null");
    assertEquals(
        datahubUpgradeHistory.getPartitions(),
        Integer.valueOf(1),
        "datahubUpgradeHistory should retain its explicit partitions value");
    // replicationFactor should be initialized from defaults since it's not explicitly set
    assertEquals(
        datahubUpgradeHistory.getReplicationFactor(),
        topicDefaults.getReplicationFactor(),
        "datahubUpgradeHistory replicationFactor should be initialized from defaults");

    // Test configProperties merging for datahubUpgradeHistory (should merge defaults with specific
    // retention)
    Map<String, String> datahubUpgradeHistoryConfig = datahubUpgradeHistory.getConfigProperties();
    assertNotNull(
        datahubUpgradeHistoryConfig, "datahubUpgradeHistory configProperties should not be null");

    // Verify that datahubUpgradeHistory inherits the default max.message.bytes
    assertEquals(
        datahubUpgradeHistoryConfig.get("max.message.bytes"),
        defaultConfig.get("max.message.bytes"),
        "datahubUpgradeHistory should inherit max.message.bytes from defaults");
    assertEquals(
        datahubUpgradeHistoryConfig.get("max.message.bytes"),
        "5242880",
        "datahubUpgradeHistory should have default max.message.bytes value");

    // Verify that datahubUpgradeHistory has its specific retention.ms value
    assertEquals(
        datahubUpgradeHistoryConfig.get("retention.ms"),
        "-1",
        "datahubUpgradeHistory should have its specific retention.ms value");

    // Verify that the configProperties map contains both default and specific properties
    assertTrue(
        datahubUpgradeHistoryConfig.containsKey("max.message.bytes"),
        "datahubUpgradeHistory should contain default max.message.bytes");

    // Test metadataChangeLogTimeseries topic (should merge defaults with specific retention)
    TopicsConfiguration.TopicConfiguration metadataChangeLogTimeseries =
        topics.get("metadataChangeLogTimeseries");
    assertNotNull(metadataChangeLogTimeseries, "metadataChangeLogTimeseries should not be null");

    // Test configProperties merging for metadataChangeLogTimeseries
    Map<String, String> metadataChangeLogTimeseriesConfig =
        metadataChangeLogTimeseries.getConfigProperties();
    assertNotNull(
        metadataChangeLogTimeseriesConfig,
        "metadataChangeLogTimeseries configProperties should not be null");

    // Verify that metadataChangeLogTimeseries inherits the default max.message.bytes
    assertEquals(
        metadataChangeLogTimeseriesConfig.get("max.message.bytes"),
        defaultConfig.get("max.message.bytes"),
        "metadataChangeLogTimeseries should inherit max.message.bytes from defaults");

    // Verify that metadataChangeLogTimeseries has its specific retention.ms value
    assertEquals(
        metadataChangeLogTimeseriesConfig.get("retention.ms"),
        "7776000000",
        "metadataChangeLogTimeseries should have its specific retention.ms value");

    // Verify that the configProperties map contains both default and specific properties
    assertTrue(
        metadataChangeLogTimeseriesConfig.containsKey("max.message.bytes"),
        "metadataChangeLogTimeseries should contain default max.message.bytes");
    assertTrue(
        metadataChangeLogTimeseriesConfig.containsKey("retention.ms"),
        "metadataChangeLogTimeseries should contain specific retention.ms");
    assertTrue(
        metadataChangeLogTimeseriesConfig.size() >= 2,
        "metadataChangeLogTimeseries configProperties should contain at least default and specific properties");
  }

  @Test
  public void testGetDataHubUsage() {
    DataHubAppConfiguration appConfig = testApplication.getDataHubAppConfig();
    KafkaConfiguration kafkaConfig = appConfig.getKafka();

    TopicsConfiguration topicsConfig = kafkaConfig.getTopics();
    assertNotNull(topicsConfig, "TopicsConfiguration should not be null");

    // Test getDataHubUsage method
    String dataHubUsage = topicsConfig.getDataHubUsage();
    assertNotNull(dataHubUsage, "getDataHubUsage should not return null");

    // Since datahubUsageEvent topic is configured in the YAML, it should return the topic name
    assertEquals(
        dataHubUsage,
        "DataHubUsageEvent_v1",
        "getDataHubUsage should return the datahubUsageEvent topic name from configuration");
  }
}
