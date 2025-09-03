package com.linkedin.datahub.upgrade.config;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.cdc.debezium.DebeziumCDCSourceSetup;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.CDCSourceConfiguration;
import com.linkedin.metadata.config.DebeziumConfiguration;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.MCLProcessingConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CDCSetupConfigTest {

  @Mock private OperationContext mockOpContext;
  @Mock private ConfigurationProvider mockConfigProvider;

  private CDCSetupConfig cdcSetupConfig;
  private MCLProcessingConfiguration mclProcessingConfig;
  private CDCSourceConfiguration cdcSourceConfig;
  private DebeziumConfiguration debeziumConfig;
  private EbeanConfiguration ebeanConfig;
  private KafkaConfiguration kafkaConfig;
  private KafkaProperties kafkaProperties;

  @BeforeMethod
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);

    cdcSetupConfig = new CDCSetupConfig();

    // Use reflection to inject the mock OperationContext
    Field opContextField = CDCSetupConfig.class.getDeclaredField("opContext");
    opContextField.setAccessible(true);
    opContextField.set(cdcSetupConfig, mockOpContext);

    // Create real configuration objects
    setupRealConfigurations();
  }

  private void setupRealConfigurations() {
    // Create real DebeziumConfiguration
    debeziumConfig = new DebeziumConfiguration();
    debeziumConfig.setName("test-connector");
    debeziumConfig.setUrl("http://localhost:8083");

    Map<String, String> connectorConfig = new HashMap<>();
    connectorConfig.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
    connectorConfig.put("database.include.list", "testdb");
    connectorConfig.put("topic.prefix", "test-prefix");
    debeziumConfig.setConfig(connectorConfig);

    // Create real CDCSourceConfiguration
    cdcSourceConfig = new CDCSourceConfiguration();
    cdcSourceConfig.setEnabled(true);
    cdcSourceConfig.setConfigureSource(true);
    cdcSourceConfig.setType("debezium");
    cdcSourceConfig.setCdcImplConfig(debeziumConfig);

    // Create real MCLProcessingConfiguration
    mclProcessingConfig = new MCLProcessingConfiguration();
    mclProcessingConfig.setCdcSource(cdcSourceConfig);

    // Create real EbeanConfiguration
    ebeanConfig = new EbeanConfiguration();
    ebeanConfig.setUrl("jdbc:mysql://localhost:3306/testdb");
    ebeanConfig.setUsername("testuser");
    ebeanConfig.setPassword("testpass");

    // Create real KafkaConfiguration
    kafkaConfig = new KafkaConfiguration();
    kafkaConfig.setBootstrapServers("localhost:9092");

    // Create real KafkaProperties
    kafkaProperties = new KafkaProperties();
    kafkaProperties.setBootstrapServers(List.of("localhost:9092"));

    // Setup ConfigurationProvider mock to return real config objects
    when(mockConfigProvider.getMclProcessing()).thenReturn(mclProcessingConfig);
    when(mockConfigProvider.getEbean()).thenReturn(ebeanConfig);
    when(mockConfigProvider.getKafka()).thenReturn(kafkaConfig);
  }

  @Test
  public void testCdcSetupNullMclProcessing() {
    when(mockConfigProvider.getMclProcessing()).thenReturn(null);

    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(mockConfigProvider, kafkaProperties);

    assertNull(result);
  }

  @Test
  public void testCdcSetupNullCdcSource() {
    mclProcessingConfig.setCdcSource(null);

    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(mockConfigProvider, kafkaProperties);

    assertNull(result);
  }

  @Test
  public void testCdcSetupCdcNotEnabled() {
    cdcSourceConfig.setEnabled(false);

    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(mockConfigProvider, kafkaProperties);

    assertNull(result);
  }

  @Test
  public void testCdcSetupConfigureSourceDisabled() {
    cdcSourceConfig.setConfigureSource(false);

    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(mockConfigProvider, kafkaProperties);

    assertNull(result);
  }

  @Test
  public void testCdcSetupDebeziumType() {
    resetToValidConfiguration();
    cdcSourceConfig.setType("debezium");

    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(mockConfigProvider, kafkaProperties);

    assertNotNull(result, "Expected DebeziumCDCSourceSetup but got null");
    assertTrue(result instanceof DebeziumCDCSourceSetup);
  }

  @Test
  public void testCdcSetupDebeziumKafkaConnectorType() {
    resetToValidConfiguration();
    cdcSourceConfig.setType("debezium-kafka-connector");

    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(mockConfigProvider, kafkaProperties);

    assertNotNull(result);
    assertTrue(result instanceof DebeziumCDCSourceSetup);
  }

  @Test
  public void testCdcSetupDebeziumTypeCaseInsensitive() {
    resetToValidConfiguration();
    cdcSourceConfig.setType("DEBEZIUM");

    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(mockConfigProvider, kafkaProperties);

    assertNotNull(result);
    assertTrue(result instanceof DebeziumCDCSourceSetup);
  }

  @Test
  public void testCdcSetupUnsupportedType() {
    cdcSourceConfig.setType("unsupported-type");

    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(mockConfigProvider, kafkaProperties);

    assertNull(result);
  }

  @Test
  public void testCdcSetupExceptionHandling() {
    cdcSourceConfig.setType("debezium");
    when(mockConfigProvider.getEbean())
        .thenThrow(new RuntimeException("Database configuration error"));

    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(mockConfigProvider, kafkaProperties);

    assertNull(result);
  }

  @Test
  public void testCdcSetupWithNullCdcType() {
    cdcSourceConfig.setType(null);

    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(mockConfigProvider, kafkaProperties);

    assertNull(result);
  }

  @Test
  public void testCdcSetupWithEmptyCdcType() {
    cdcSourceConfig.setType("");

    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(mockConfigProvider, kafkaProperties);

    assertNull(result);
  }

  @Test
  public void testDebeziumCDCSetupConfigurationInjection() {
    resetToValidConfiguration();
    cdcSourceConfig.setType("debezium");

    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(mockConfigProvider, kafkaProperties);

    assertNotNull(result);
    DebeziumCDCSourceSetup debeziumSetup = (DebeziumCDCSourceSetup) result;
    assertEquals(debeziumSetup.getCdcType(), "debezium");
  }

  @Test
  public void testCdcSetupWithNullDebeziumConfig() {
    resetToValidConfiguration();
    cdcSourceConfig.setType("debezium");
    cdcSourceConfig.setCdcImplConfig(null);

    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(mockConfigProvider, kafkaProperties);

    assertNull(result);
  }

  private void resetToValidConfiguration() {
    // Recreate DebeziumConfiguration fresh to avoid any issues
    debeziumConfig = new DebeziumConfiguration();
    debeziumConfig.setName("test-connector");
    debeziumConfig.setUrl("http://localhost:8083");

    Map<String, String> connectorConfig = new HashMap<>();
    connectorConfig.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
    connectorConfig.put("database.include.list", "testdb");
    connectorConfig.put("topic.prefix", "test-prefix");
    debeziumConfig.setConfig(connectorConfig);

    // Recreate CDCSourceConfiguration to ensure clean state
    cdcSourceConfig = new CDCSourceConfiguration();
    cdcSourceConfig.setEnabled(true);
    cdcSourceConfig.setConfigureSource(true);
    cdcSourceConfig.setType("debezium");
    cdcSourceConfig.setCdcImplConfig(debeziumConfig);

    // Recreate MCLProcessingConfiguration with new CDC source config
    mclProcessingConfig = new MCLProcessingConfiguration();
    mclProcessingConfig.setCdcSource(cdcSourceConfig);

    // Reset ConfigurationProvider mock behavior
    when(mockConfigProvider.getMclProcessing()).thenReturn(mclProcessingConfig);
    when(mockConfigProvider.getEbean()).thenReturn(ebeanConfig);
    when(mockConfigProvider.getKafka()).thenReturn(kafkaConfig);
  }
}
