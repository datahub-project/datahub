package com.linkedin.datahub.upgrade.system.cdc.debezium;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.DebeziumConfiguration;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConfigureDebeziumConnectorStepTest {

  @Mock private OperationContext mockOpContext;

  private DebeziumConfiguration debeziumConfig;
  private EbeanConfiguration ebeanConfig;
  private KafkaConfiguration kafkaConfig;
  private KafkaProperties kafkaProperties;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

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
    kafkaProperties.setBootstrapServers(Arrays.asList("localhost:9092"));
  }

  @AfterMethod
  public void tearDown() {
    System.clearProperty("EBEAN_DATASOURCE_HOST");
    System.clearProperty("DATAHUB_DB_NAME");
  }

  @Test
  public void testId() {
    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);
    assertEquals(step.id(), "ConfigureDebeziumConnectorStep");
  }

  @Test
  public void testConstructor() {
    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);
    assertNotNull(step);
  }

  @Test
  public void testExecutableReturnsFunction() {
    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);
    assertNotNull(step.executable());
  }

  @Test
  public void testWithPostgresConfiguration() {
    ebeanConfig.setUrl("jdbc:postgresql://pg-host:5432/testdb");

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);
    assertNotNull(step);
  }

  @Test
  public void testWithMultipleKafkaBrokers() {
    kafkaProperties.setBootstrapServers(
        Arrays.asList("broker1:9092", "broker2:9092", "broker3:9092"));

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);
    assertNotNull(step);
  }

  @Test
  public void testWithConfiguredUrl() {
    debeziumConfig.setUrl("http://test-connect:8083");

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);
    assertNotNull(step);
  }

  @Test
  public void testWithEnvironmentVariables() {
    System.setProperty("EBEAN_DATASOURCE_HOST", "test-host:3306");
    System.setProperty("DATAHUB_DB_NAME", "test-database");

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);
    assertNotNull(step);
  }

  @Test
  public void testKafkaConfigurationPrecedence() {
    kafkaConfig.setBootstrapServers("kafka-config-server:9092");
    kafkaProperties.setBootstrapServers(Arrays.asList("kafka-props-server:9092"));

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);
    assertNotNull(step);
  }

  @Test
  public void testKafkaPropertiesFallback() {
    kafkaConfig.setBootstrapServers(null);
    kafkaProperties.setBootstrapServers(Arrays.asList("fallback-server:9092"));

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);
    assertNotNull(step);
  }

  @Test
  public void testConnectorNameFromConfig() {
    debeziumConfig.setName("custom-connector-name");

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);
    assertNotNull(step);
  }

  @Test
  public void testWithMissingConfig() {
    debeziumConfig.setConfig(null);

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);
    assertNotNull(step);
  }

  @Test
  public void testBuildConnectorConfiguration() {
    debeziumConfig.setConfig(null);

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    assertTrue(config.size() > 0);
    assertTrue(config.containsKey("connector.class"));
    assertTrue(config.containsKey("transforms"));
    assertTrue(config.containsKey("database.user"));
    assertTrue(config.containsKey("schema.history.internal.kafka.bootstrap.servers"));
  }

  @Test
  public void testBuildConnectorConfigurationWithExplicitOverrides() {
    Map<String, String> overrides = new HashMap<>();
    overrides.put("connector.class", "io.debezium.connector.postgresql.PostgreSqlConnector");
    overrides.put("custom.property", "override-value");
    debeziumConfig.setConfig(overrides);

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            mockOpContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    assertEquals(
        config.get("connector.class"), "io.debezium.connector.postgresql.PostgreSqlConnector");
    assertEquals(config.get("custom.property"), "override-value");
  }
}
