package com.linkedin.metadata.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.io.IOException;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * Integration test that loads the full application.yaml configuration and verifies that CDC-related
 * configurations are properly parsed and structured.
 */
@SpringBootTest(classes = DataHubTestApplication.class)
public class DataHubAppConfigurationTest extends AbstractTestNGSpringContextTests {

  @Autowired private DataHubTestApplication testApplication;

  @Test
  public void testInit() {
    assertNotNull(testApplication);
  }

  @Test
  public void testMCPBatchDefaults() {
    assertFalse(
        testApplication
            .getDataHubAppConfig()
            .getMetadataChangeProposal()
            .getConsumer()
            .getBatch()
            .isEnabled());
    assertEquals(
        testApplication
            .getDataHubAppConfig()
            .getMetadataChangeProposal()
            .getConsumer()
            .getBatch()
            .getSize(),
        15744000);
  }

  @Test
  public void testCustomSearchConfiguration() throws IOException {
    assertNotNull(testApplication.getDataHubAppConfig());
    assertNotNull(testApplication.getDataHubAppConfig().getElasticSearch());
    assertNotNull(testApplication.getDataHubAppConfig().getElasticSearch().getSearch());
    assertNotNull(testApplication.getDataHubAppConfig().getElasticSearch().getSearch().getCustom());
    assertNotNull(
        testApplication
            .getDataHubAppConfig()
            .getElasticSearch()
            .getSearch()
            .getCustom()
            .resolve(new YAMLMapper()));
  }

  @Test
  public void testCDCConfigurationLoading() {
    DataHubAppConfiguration appConfig = testApplication.getDataHubAppConfig();
    assertNotNull(appConfig);

    // Verify MCL Processing configuration exists
    MCLProcessingConfiguration mclProcessing = appConfig.getMclProcessing();
    assertNotNull(
        mclProcessing, "MCL Processing configuration should be loaded from application.yaml");

    // Verify CDC Source configuration exists
    CDCSourceConfiguration cdcSource = mclProcessing.getCdcSource();
    assertNotNull(cdcSource, "CDC Source configuration should be loaded from application.yaml");

    // Verify CDC configuration properties from application.yaml
    assertFalse(
        cdcSource.isEnabled(),
        "CDC should be disabled by default (CDC_MCL_PROCESSING_ENABLED:false)");
    assertEquals(
        cdcSource.getConverter(),
        "com.datahub.cdc.MySQLToMCLConverter",
        "CDC converter should match application.yaml configuration");
    assertFalse(
        cdcSource.isConfigureSource(), "CDC configureSource should be false from application.yaml");
    assertEquals(
        cdcSource.getType(),
        "debezium-kafka-connector",
        "CDC type should match application.yaml configuration");
  }

  @Test
  public void testDebeziumConfigurationLoading() {
    DataHubAppConfiguration appConfig = testApplication.getDataHubAppConfig();
    CDCSourceConfiguration cdcSource = appConfig.getMclProcessing().getCdcSource();

    // Verify Debezium configuration exists
    DebeziumConfiguration debeziumConfig = cdcSource.getDebeziumConfig();
    assertNotNull(debeziumConfig, "Debezium configuration should be loaded from application.yaml");

    // Verify Debezium properties from application.yaml
    assertEquals(
        debeziumConfig.getName(),
        "datahub-cdc-connector",
        "Debezium connector name should match application.yaml (DATAHUB_CDC_CONNECTOR_NAME default)");
    assertEquals(
        debeziumConfig.getUrl(),
        "http://kafka-connect:8083",
        "Debezium URL should match application.yaml (CDC_KAFKA_CONNECT_URL default)");

    assertEquals(
        debeziumConfig.getRequestTimeoutMillis(),
        10000,
        "Debezium request timeout should match application.yaml (CDC_KAFKA_CONNECT_REQUEST_TIMEOUT default)");
  }

  @Test
  public void testDebeziumConfigurationMap() {
    DataHubAppConfiguration appConfig = testApplication.getDataHubAppConfig();
    CDCSourceConfiguration cdcSource = appConfig.getMclProcessing().getCdcSource();
    DebeziumConfiguration debeziumConfig = cdcSource.getDebeziumConfig();

    // Verify Debezium config map exists and is properly structured
    Map<String, String> config = debeziumConfig.getConfig();
    assertNotNull(config, "Debezium config map should be loaded from application.yaml");
    assertFalse(config.isEmpty(), "Debezium config map should contain configuration properties");

    // With Map<String, String>, dotted keys should be preserved as flat keys
    assertTrue(
        config.containsKey("connector.class"), "Config should contain 'connector.class' key");

    assertEquals(
        config.get("connector.class"),
        "io.debezium.connector.mysql.MySqlConnector",
        "Debezium connector class should match application.yaml configuration");
  }

  @Test
  public void testCDCConfigurationHierarchy() {
    // Test the complete configuration hierarchy: DataHubAppConfig -> MCLProcessing -> CDCSource ->
    // DebeziumConfig
    DataHubAppConfiguration appConfig = testApplication.getDataHubAppConfig();

    // Verify we can navigate the full hierarchy
    MCLProcessingConfiguration mclProcessing = appConfig.getMclProcessing();
    CDCSourceConfiguration cdcSource = mclProcessing.getCdcSource();
    DebeziumConfiguration debeziumConfig = cdcSource.getDebeziumConfig();

    assertNotNull(appConfig, "DataHubAppConfiguration should be loaded");
    assertNotNull(mclProcessing, "MCLProcessingConfiguration should be accessible");
    assertNotNull(cdcSource, "CDCSourceConfiguration should be accessible");
    assertNotNull(debeziumConfig, "DebeziumConfiguration should be accessible");

    // Verify getCdcImplConfig() works correctly with loaded configuration
    Object implConfig = cdcSource.getCdcImplConfig();
    assertNotNull(
        implConfig,
        "getCdcImplConfig() should return configuration for debezium-kafka-connector type");
    assertTrue(
        implConfig instanceof DebeziumConfiguration,
        "getCdcImplConfig() should return DebeziumConfiguration instance");
    assertEquals(
        implConfig,
        debeziumConfig,
        "getCdcImplConfig() should return the same instance as getDebeziumConfig()");
  }

  @Test
  public void testCDCConfigurationTypeSafety() {
    // Test type-safe configuration access
    DataHubAppConfiguration appConfig = testApplication.getDataHubAppConfig();
    CDCSourceConfiguration cdcSource = appConfig.getMclProcessing().getCdcSource();

    // Test that type-based configuration retrieval works
    Object implConfig = cdcSource.getCdcImplConfig();
    assertNotNull(implConfig);

    // Verify we can safely cast to DebeziumConfiguration
    if (implConfig instanceof DebeziumConfiguration) {
      DebeziumConfiguration debeziumConfig = (DebeziumConfiguration) implConfig;
      assertNotNull(debeziumConfig.getName());
      assertNotNull(debeziumConfig.getUrl());
      assertNotNull(debeziumConfig.getConfig());
    } else {
      throw new AssertionError("Expected DebeziumConfiguration but got: " + implConfig.getClass());
    }
  }
}
