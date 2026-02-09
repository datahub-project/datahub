package com.linkedin.datahub.upgrade.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.cdc.CDCSourceSetup;
import com.linkedin.datahub.upgrade.system.cdc.debezium.DebeziumCDCSourceSetup;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.CDCSourceConfiguration;
import com.linkedin.metadata.config.DebeziumConfiguration;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.MCLProcessingConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.ProducerConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
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

  private CDCSetupConfig cdcSetupConfig;
  private CDCSourceConfiguration cdcSourceConfig;
  private DebeziumConfiguration debeziumConfig;
  private EbeanConfiguration ebeanConfig;
  private KafkaConfiguration kafkaConfig;
  private KafkaProperties kafkaProperties;

  @BeforeMethod
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);

    cdcSetupConfig = new CDCSetupConfig();

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
    cdcSourceConfig.setType("debezium-kafka-connector");
    cdcSourceConfig.setCdcImplConfig(debeziumConfig);

    // Create real EbeanConfiguration
    ebeanConfig = new EbeanConfiguration();
    ebeanConfig.setUrl("jdbc:mysql://localhost:3306/testdb");
    ebeanConfig.setUsername("testuser");
    ebeanConfig.setPassword("testpass");

    // Create real KafkaConfiguration
    kafkaConfig = new KafkaConfiguration();
    kafkaConfig.setProducer(new ProducerConfiguration());
    kafkaConfig.setBootstrapServers("localhost:9092");

    // Create real KafkaProperties
    kafkaProperties = new KafkaProperties();
    kafkaProperties.setBootstrapServers(List.of("localhost:9092"));
  }

  @Test
  public void testCdcSetupWithNullList() {
    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(null);
    assertNull(result);
  }

  @Test
  public void testCdcSetupWithEmptyList() {
    List<CDCSourceSetup> emptyList = Collections.emptyList();
    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(emptyList);
    assertNull(result);
  }

  @Test
  public void testCdcSetupWithSingleImplementation() {
    // Create mock ConfigurationProvider
    ConfigurationProvider mockConfigProvider =
        org.mockito.Mockito.mock(ConfigurationProvider.class);
    MCLProcessingConfiguration mclConfig = new MCLProcessingConfiguration();
    mclConfig.setCdcSource(cdcSourceConfig);

    org.mockito.Mockito.when(mockConfigProvider.getMclProcessing()).thenReturn(mclConfig);
    org.mockito.Mockito.when(mockConfigProvider.getEbean()).thenReturn(ebeanConfig);
    org.mockito.Mockito.when(mockConfigProvider.getKafka()).thenReturn(kafkaConfig);

    DebeziumCDCSourceSetup debeziumSetup =
        new DebeziumCDCSourceSetup(mockOpContext, mockConfigProvider, kafkaProperties);

    List<CDCSourceSetup> cdcSetups = List.of(debeziumSetup);
    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(cdcSetups);

    assertNotNull(result);
    assertTrue(result instanceof DebeziumCDCSourceSetup);
    assertEquals(result.id(), "DebeziumCDCSetup");
  }

  @Test
  public void testCdcSetupWithMultipleImplementations() {
    // Create mock ConfigurationProvider
    ConfigurationProvider mockConfigProvider =
        org.mockito.Mockito.mock(ConfigurationProvider.class);
    MCLProcessingConfiguration mclConfig = new MCLProcessingConfiguration();
    mclConfig.setCdcSource(cdcSourceConfig);

    org.mockito.Mockito.when(mockConfigProvider.getMclProcessing()).thenReturn(mclConfig);
    org.mockito.Mockito.when(mockConfigProvider.getEbean()).thenReturn(ebeanConfig);
    org.mockito.Mockito.when(mockConfigProvider.getKafka()).thenReturn(kafkaConfig);

    DebeziumCDCSourceSetup debeziumSetup1 =
        new DebeziumCDCSourceSetup(mockOpContext, mockConfigProvider, kafkaProperties);

    DebeziumCDCSourceSetup debeziumSetup2 =
        new DebeziumCDCSourceSetup(mockOpContext, mockConfigProvider, kafkaProperties);

    List<CDCSourceSetup> cdcSetups = new ArrayList<>();
    cdcSetups.add(debeziumSetup1);
    cdcSetups.add(debeziumSetup2);

    // Should return the first one and log a warning
    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(cdcSetups);

    assertNotNull(result);
    assertTrue(result instanceof DebeziumCDCSourceSetup);
    assertEquals(result, debeziumSetup1); // Should be the first one
  }

  @Test
  public void testCdcSetupReturnsFirstFromList() {
    // Create mock ConfigurationProvider
    ConfigurationProvider mockConfigProvider =
        org.mockito.Mockito.mock(ConfigurationProvider.class);
    MCLProcessingConfiguration mclConfig = new MCLProcessingConfiguration();
    mclConfig.setCdcSource(cdcSourceConfig);

    org.mockito.Mockito.when(mockConfigProvider.getMclProcessing()).thenReturn(mclConfig);
    org.mockito.Mockito.when(mockConfigProvider.getEbean()).thenReturn(ebeanConfig);
    org.mockito.Mockito.when(mockConfigProvider.getKafka()).thenReturn(kafkaConfig);

    DebeziumCDCSourceSetup debeziumSetup =
        new DebeziumCDCSourceSetup(mockOpContext, mockConfigProvider, kafkaProperties);

    List<CDCSourceSetup> cdcSetups = List.of(debeziumSetup);
    BlockingSystemUpgrade result = cdcSetupConfig.cdcSetup(cdcSetups);

    assertNotNull(result);
    assertEquals(result, debeziumSetup);
    assertEquals(((CDCSourceSetup) result).getCdcType(), "debezium");
  }
}
