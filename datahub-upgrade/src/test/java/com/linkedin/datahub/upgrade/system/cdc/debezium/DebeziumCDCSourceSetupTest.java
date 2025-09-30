package com.linkedin.datahub.upgrade.system.cdc.debezium;

import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.CDCSourceConfiguration;
import com.linkedin.metadata.config.DebeziumConfiguration;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.MCLProcessingConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DebeziumCDCSourceSetupTest {

  @Mock private OperationContext mockOpContext;
  @Mock private ConfigurationProvider mockConfigProvider;

  private CDCSourceConfiguration cdcSourceConfig;
  private DebeziumConfiguration debeziumConfig;
  private EbeanConfiguration ebeanConfig;
  private KafkaConfiguration kafkaConfig;
  private KafkaProperties kafkaProperties;
  private MCLProcessingConfiguration mclProcessingConfig;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Create real CDCSourceConfiguration with test data
    cdcSourceConfig = new CDCSourceConfiguration();
    cdcSourceConfig.setEnabled(true);
    cdcSourceConfig.setConfigureSource(true);
    cdcSourceConfig.setType("debezium-kafka-connector");

    // Create DebeziumConfiguration for the test
    debeziumConfig = new DebeziumConfiguration();
    debeziumConfig.setName("test-connector");
    debeziumConfig.setUrl("http://localhost:8083");

    // Base configuration for MySQL connector
    Map<String, String> config = new HashMap<>();
    config.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
    config.put("database.include.list", "testdb");
    config.put("table.include.list", "*.metadata_aspect_v2");
    config.put("topic.prefix", "test-prefix");
    debeziumConfig.setConfig(config);

    cdcSourceConfig.setCdcImplConfig(debeziumConfig);

    // Create real MCLProcessingConfiguration
    mclProcessingConfig = new MCLProcessingConfiguration();
    mclProcessingConfig.setCdcSource(cdcSourceConfig);

    // Create real EbeanConfiguration with test data
    ebeanConfig = new EbeanConfiguration();
    ebeanConfig.setUrl("jdbc:mysql://localhost:3306/testdb");
    ebeanConfig.setUsername("testuser");
    ebeanConfig.setPassword("testpass");

    // Create real KafkaConfiguration with test data
    kafkaConfig = new KafkaConfiguration();
    kafkaConfig.setBootstrapServers("localhost:9092");

    // Create real KafkaProperties with test data
    kafkaProperties = new KafkaProperties();
    kafkaProperties.setBootstrapServers(Arrays.asList("localhost:9092"));

    // Setup ConfigurationProvider mock
    when(mockConfigProvider.getMclProcessing()).thenReturn(mclProcessingConfig);
    when(mockConfigProvider.getEbean()).thenReturn(ebeanConfig);
    when(mockConfigProvider.getKafka()).thenReturn(kafkaConfig);
  }

  @Test
  public void testId() {
    DebeziumCDCSourceSetup setup =
        new DebeziumCDCSourceSetup(mockOpContext, mockConfigProvider, kafkaProperties);
    assertEquals(setup.id(), "DebeziumCDCSetup");
  }

  @Test
  public void testStepsCreation() {
    DebeziumCDCSourceSetup setup =
        new DebeziumCDCSourceSetup(mockOpContext, mockConfigProvider, kafkaProperties);

    List<UpgradeStep> steps = setup.steps();
    assertNotNull(steps);
    assertEquals(steps.size(), 2);

    // Verify the steps are of the correct types
    assertTrue(steps.get(0) instanceof WaitForDebeziumReadyStep);
    assertTrue(steps.get(1) instanceof ConfigureDebeziumConnectorStep);
  }

  @Test
  public void testStepsHaveCorrectIds() {
    DebeziumCDCSourceSetup setup =
        new DebeziumCDCSourceSetup(mockOpContext, mockConfigProvider, kafkaProperties);

    List<UpgradeStep> steps = setup.steps();

    // Test that the steps have the expected IDs
    assertEquals(steps.get(0).id(), "WaitForDebeziumReadyStep");
    assertEquals(steps.get(1).id(), "ConfigureDebeziumConnectorStep");
  }

  @Test
  public void testCanRun() {
    DebeziumCDCSourceSetup setup =
        new DebeziumCDCSourceSetup(mockOpContext, mockConfigProvider, kafkaProperties);
    assertTrue(setup.canRun());
  }

  @Test
  public void testCanRunWithInvalidConfig() {
    // Test with invalid config
    debeziumConfig.setConfig(null);

    DebeziumCDCSourceSetup setup =
        new DebeziumCDCSourceSetup(mockOpContext, mockConfigProvider, kafkaProperties);
    // This should return false because the config is not valid
    assertTrue(!setup.canRun()); // We expect this to be false for invalid config
  }

  @Test
  public void testGetCdcType() {
    DebeziumCDCSourceSetup setup =
        new DebeziumCDCSourceSetup(mockOpContext, mockConfigProvider, kafkaProperties);
    assertEquals(setup.getCdcType(), DebeziumCDCSourceSetup.DEBEZIUM_TYPE);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testConstructorWithNullDebeziumConfig() {
    cdcSourceConfig.setCdcImplConfig(null);

    new DebeziumCDCSourceSetup(mockOpContext, mockConfigProvider, kafkaProperties);
  }

  @Test
  public void testCanRunWithMissingConnectorClass() {
    Map<String, String> config = new HashMap<>();
    config.put("topic.prefix", "test-prefix");
    debeziumConfig.setConfig(config);

    DebeziumCDCSourceSetup setup =
        new DebeziumCDCSourceSetup(mockOpContext, mockConfigProvider, kafkaProperties);

    assertFalse(setup.canRun());
  }

  @Test
  public void testIdWithNullConnectorName() {
    debeziumConfig.setName(null);

    DebeziumCDCSourceSetup setup =
        new DebeziumCDCSourceSetup(mockOpContext, mockConfigProvider, kafkaProperties);

    assertEquals(setup.id(), "DebeziumCDCSetup");
  }
}
