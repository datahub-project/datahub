package com.linkedin.datahub.upgrade.system.cdc.debezium;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.DebeziumConfiguration;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConfigureDebeziumConnectorStepTest {

  @Mock private OperationContext mockOpContext;

  private DebeziumConfiguration debeziumConfig;
  private EbeanConfiguration ebeanConfig;
  private KafkaConfiguration kafkaConfig;
  private KafkaProperties kafkaProperties;

  // Test subclass to access protected methods
  private static class TestableConfigureDebeziumConnectorStep
      extends ConfigureDebeziumConnectorStep {
    public TestableConfigureDebeziumConnectorStep(
        OperationContext opContext,
        DebeziumConfiguration debeziumConfig,
        EbeanConfiguration ebeanConfig,
        KafkaConfiguration kafkaConfig,
        KafkaProperties kafkaProperties) {
      super(opContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);
    }

    @Override
    public Map<String, Object> buildConnectorConfiguration() {
      return super.buildConnectorConfiguration();
    }
  }

  @BeforeMethod
  public void setUp() throws Exception {
    try (var mocks = MockitoAnnotations.openMocks(this)) {
      debeziumConfig = new DebeziumConfiguration();
      debeziumConfig.setName("test-connector");
      debeziumConfig.setUrl("http://localhost:8083");

      Map<String, String> config = new HashMap<>();
      config.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
      config.put("database.include.list", "testdb");
      config.put("table.include.list", "*.metadata_aspect_v2");
      config.put("topic.prefix", "test-prefix");
      debeziumConfig.setConfig(config);

      ebeanConfig = new EbeanConfiguration();
      ebeanConfig.setUrl("jdbc:mysql://localhost:3306/testdb");
      ebeanConfig.setUsername("testuser");
      ebeanConfig.setPassword("testpass");

      kafkaConfig = new KafkaConfiguration();
      kafkaConfig.setBootstrapServers("localhost:9092");

      kafkaProperties = new KafkaProperties();
      kafkaProperties.setBootstrapServers(List.of("localhost:9092"));
    }
  }

  @Test
  public void testBasicStepConstruction() {
    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    assertNotNull(step);
    assertEquals(step.id(), "ConfigureDebeziumConnectorStep");
    assertNotNull(step.executable());
  }

  @Test
  public void testBuildConnectorConfiguration() {
    TestableConfigureDebeziumConnectorStep step =
        new TestableConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    assertFalse(config.isEmpty());
    assertTrue(config.containsKey("connector.class"));
    assertEquals(config.get("connector.class"), "io.debezium.connector.mysql.MySqlConnector");
    assertTrue(config.containsKey("schema.history.internal.kafka.bootstrap.servers"));
  }

  @Test
  public void testBuildConnectorConfigurationWithExplicitOverrides() {
    Map<String, String> overrides = new HashMap<>();
    overrides.put("connector.class", "io.debezium.connector.postgresql.PostgreSqlConnector");
    overrides.put("custom.property", "override-value");
    debeziumConfig.setConfig(overrides);

    TestableConfigureDebeziumConnectorStep step =
        new TestableConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    assertEquals(
        config.get("connector.class"), "io.debezium.connector.postgresql.PostgreSqlConnector");
    assertEquals(config.get("custom.property"), "override-value");
  }

  @Test
  public void testKafkaConfigurationPrecedence() {
    kafkaConfig.setBootstrapServers("kafka-config-server:9092");
    kafkaProperties.setBootstrapServers(List.of("kafka-props-server:9092"));

    TestableConfigureDebeziumConnectorStep step =
        new TestableConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    // Verify kafkaConfig takes precedence over kafkaProperties
    assertEquals(
        config.get("schema.history.internal.kafka.bootstrap.servers"), "kafka-config-server:9092");
  }

  @Test
  public void testKafkaPropertiesFallback() {
    kafkaConfig.setBootstrapServers(null);
    kafkaProperties.setBootstrapServers(List.of("fallback-server:9092"));

    TestableConfigureDebeziumConnectorStep step =
        new TestableConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    // Verify fallback to kafkaProperties when kafkaConfig is null
    assertEquals(
        config.get("schema.history.internal.kafka.bootstrap.servers"), "fallback-server:9092");
  }

  @Test
  public void testMultipleKafkaBrokersConfiguration() {
    kafkaConfig.setBootstrapServers(null);
    kafkaProperties.setBootstrapServers(List.of("broker1:9092", "broker2:9092", "broker3:9092"));

    TestableConfigureDebeziumConnectorStep step =
        new TestableConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    String brokers = (String) config.get("schema.history.internal.kafka.bootstrap.servers");
    assertTrue(brokers.contains("broker1:9092"));
    assertTrue(brokers.contains("broker2:9092"));
    assertTrue(brokers.contains("broker3:9092"));
  }

  @Test
  public void testConnectorConfigurationWithTopicPrefix() {
    TestableConfigureDebeziumConnectorStep step =
        new TestableConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    // Should include topic prefix from debezium config
    assertEquals(config.get("topic.prefix"), "test-prefix");

    // Schema history topic is only set when kafka bootstrap servers are available
    // Since we trust Spring injection, we verify topic prefix is preserved in config
    assertTrue(config.containsKey("topic.prefix"));
  }

  @Test
  public void testMissingKafkaConfiguration() {
    kafkaConfig.setBootstrapServers(null);
    kafkaProperties.setBootstrapServers(null);

    TestableConfigureDebeziumConnectorStep step =
        new TestableConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    // Should still create config but may not have kafka bootstrap servers
    assertFalse(config.isEmpty());
    assertTrue(config.containsKey("connector.class"));
  }

  @Test
  public void testDebeziumConfigurationOverridesDefaults() {
    Map<String, String> overrides = new HashMap<>();
    overrides.put("database.server.name", "custom-server");
    overrides.put("database.server.id", "12345");
    overrides.put("snapshot.mode", "initial");
    debeziumConfig.setConfig(overrides);

    TestableConfigureDebeziumConnectorStep step =
        new TestableConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    // Verify explicit overrides are preserved
    assertEquals(config.get("database.server.name"), "custom-server");
    assertEquals(config.get("database.server.id"), "12345");
    assertEquals(config.get("snapshot.mode"), "initial");
  }
}
