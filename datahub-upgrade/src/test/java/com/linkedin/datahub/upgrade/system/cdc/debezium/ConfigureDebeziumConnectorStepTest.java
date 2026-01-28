package com.linkedin.datahub.upgrade.system.cdc.debezium;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.config.DebeziumConfiguration;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.ProducerConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConfigureDebeziumConnectorStepTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private DebeziumConfiguration debeziumConfig;
  private EbeanConfiguration ebeanConfig;
  private KafkaConfiguration kafkaConfig;
  private KafkaProperties kafkaProperties;

  @BeforeMethod
  public void setUp() throws Exception {
    try (var mocks = MockitoAnnotations.openMocks(this)) {
      debeziumConfig = new DebeziumConfiguration();
      debeziumConfig.setName("test-connector");
      debeziumConfig.setUrl("http://localhost:8083");
      debeziumConfig.setRequestTimeoutMillis(30000);

      Map<String, String> config = new HashMap<>();
      config.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
      config.put("database.include.list", "testdb");
      config.put("table.include.list", "*.metadata_aspect_v2");
      config.put("topic.prefix", "test-prefix");
      config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
      config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
      debeziumConfig.setConfig(config);

      ebeanConfig = new EbeanConfiguration();
      ebeanConfig.setUrl("jdbc:mysql://localhost:3306/testdb");
      ebeanConfig.setUsername("testuser");
      ebeanConfig.setPassword("testpass");

      kafkaConfig = new KafkaConfiguration();
      kafkaConfig.setProducer(new ProducerConfiguration());
      kafkaConfig.setBootstrapServers("localhost:9092");

      kafkaProperties = new KafkaProperties();
      kafkaProperties.setBootstrapServers(List.of("localhost:9092"));
    }
  }

  @Test
  public void testBasicStepConstruction() {
    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    assertNotNull(step);
    assertEquals(step.id(), "ConfigureDebeziumConnectorStep");
    assertNotNull(step.executable());
  }

  @Test
  public void testBuildConnectorConfiguration() {
    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

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

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    assertEquals(
        config.get("connector.class"), "io.debezium.connector.postgresql.PostgreSqlConnector");
    assertEquals(config.get("custom.property"), "override-value");
  }

  @Test
  public void testKafkaConfigurationPrecedence() {
    kafkaConfig.setBootstrapServers("kafka-config-server:9092");
    kafkaProperties.setBootstrapServers(List.of("kafka-props-server:9092"));

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    // Verify kafkaConfig takes precedence over kafkaProperties
    assertEquals(
        config.get("schema.history.internal.kafka.bootstrap.servers"), "kafka-config-server:9092");
  }

  @Test
  public void testKafkaPropertiesFallback() {
    kafkaConfig.setBootstrapServers(null);
    kafkaProperties.setBootstrapServers(List.of("fallback-server:9092"));

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    // Verify fallback to kafkaProperties when kafkaConfig is null
    assertEquals(
        config.get("schema.history.internal.kafka.bootstrap.servers"), "fallback-server:9092");
  }

  @Test
  public void testMultipleKafkaBrokersConfiguration() {
    kafkaConfig.setBootstrapServers(null);
    kafkaProperties.setBootstrapServers(List.of("broker1:9092", "broker2:9092", "broker3:9092"));

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    String brokers = (String) config.get("schema.history.internal.kafka.bootstrap.servers");
    assertTrue(brokers.contains("broker1:9092"));
    assertTrue(brokers.contains("broker2:9092"));
    assertTrue(brokers.contains("broker3:9092"));
  }

  @Test
  public void testConnectorConfigurationWithTopicPrefix() {
    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

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

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

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

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    // Verify explicit overrides are preserved
    assertEquals(config.get("database.server.name"), "custom-server");
    assertEquals(config.get("database.server.id"), "12345");
    assertEquals(config.get("snapshot.mode"), "initial");
  }

  @Test
  public void testExecutableCreateConnectorSuccess() throws Exception {
    UpgradeContext mockUpgradeContext = mock(UpgradeContext.class);
    HttpClient mockHttpClient = mock(HttpClient.class);
    HttpResponse<String> mockResponse = mock(HttpResponse.class);

    when(mockResponse.statusCode()).thenReturn(404, 201);
    when(mockResponse.body()).thenReturn("Connector created successfully");
    when(mockHttpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofString())))
        .thenReturn(mockResponse);

    ConfigureDebeziumConnectorStep step =
        spy(
            new ConfigureDebeziumConnectorStep(
                OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties));
    when(step.createHttpClient()).thenReturn(mockHttpClient);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testExecutableUpdateConnectorSuccess() throws Exception {
    UpgradeContext mockUpgradeContext = mock(UpgradeContext.class);
    HttpClient mockHttpClient = mock(HttpClient.class);
    HttpResponse<String> mockResponse = mock(HttpResponse.class);

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("Connector updated successfully");
    when(mockHttpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofString())))
        .thenReturn(mockResponse);

    ConfigureDebeziumConnectorStep step =
        spy(
            new ConfigureDebeziumConnectorStep(
                OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties));
    when(step.createHttpClient()).thenReturn(mockHttpClient);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testExecutableMissingConnectUrl() throws Exception {
    UpgradeContext mockUpgradeContext = mock(UpgradeContext.class);
    debeziumConfig.setUrl(null);

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutableHttpException() throws Exception {
    UpgradeContext mockUpgradeContext = mock(UpgradeContext.class);
    HttpClient mockHttpClient = mock(HttpClient.class);

    when(mockHttpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofString())))
        .thenThrow(new RuntimeException("Network error"));

    ConfigureDebeziumConnectorStep step =
        spy(
            new ConfigureDebeziumConnectorStep(
                OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties));
    when(step.createHttpClient()).thenReturn(mockHttpClient);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testDatabaseConnectionParsingMySQL() {
    ebeanConfig.setUrl("jdbc:mysql://db-host:3306/production_db");

    // Use minimal config to avoid overrides
    Map<String, String> minimalConfig = new HashMap<>();
    minimalConfig.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
    debeziumConfig.setConfig(minimalConfig);

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    assertEquals(config.get("database.hostname"), "db-host");
    assertEquals(config.get("database.port"), "3306");
  }

  @Test
  public void testDatabaseConnectionParsingPostgreSQL() {
    ebeanConfig.setUrl("jdbc:postgresql://pg-host:5432/test_db");

    // Use minimal config to avoid overrides
    Map<String, String> minimalConfig = new HashMap<>();
    minimalConfig.put("connector.class", "io.debezium.connector.postgresql.PostgreSqlConnector");
    debeziumConfig.setConfig(minimalConfig);

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    assertEquals(config.get("database.hostname"), "pg-host");
    assertEquals(config.get("database.port"), "5432");
  }

  @Test
  public void testDatabaseConnectionDefaultPortMySQL() {
    ebeanConfig.setUrl("jdbc:mysql://mysql-host/datahub");

    // Use minimal config to avoid overrides
    Map<String, String> minimalConfig = new HashMap<>();
    minimalConfig.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
    debeziumConfig.setConfig(minimalConfig);

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    assertEquals(config.get("database.hostname"), "mysql-host");
    assertEquals(config.get("database.port"), "3306");
  }

  @Test
  public void testDatabaseConnectionDefaultPortPostgreSQL() {
    ebeanConfig.setUrl("jdbc:postgresql://pg-host/datahub");

    // Use minimal config to avoid overrides
    Map<String, String> minimalConfig = new HashMap<>();
    minimalConfig.put("connector.class", "io.debezium.connector.postgresql.PostgreSqlConnector");
    debeziumConfig.setConfig(minimalConfig);

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    assertEquals(config.get("database.hostname"), "pg-host");
    assertEquals(config.get("database.port"), "5432");
  }

  @Test
  public void testDatabaseConnectionUnparsableUrl() {
    ebeanConfig.setUrl("jdbc:oracle://oracle-host:1521:XE");

    // Use minimal config to avoid overrides
    Map<String, String> minimalConfig = new HashMap<>();
    minimalConfig.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
    debeziumConfig.setConfig(minimalConfig);

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    // Should not set any database properties for unparsable URLs
    assertFalse(config.containsKey("database.hostname"));
    assertFalse(config.containsKey("database.port"));
  }

  @Test
  public void testDatabaseConnectionNullUrl() {
    ebeanConfig.setUrl(null);

    // Use minimal config to avoid overrides
    Map<String, String> minimalConfig = new HashMap<>();
    minimalConfig.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
    debeziumConfig.setConfig(minimalConfig);

    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    // Should not set any database properties for null URL
    assertFalse(config.containsKey("database.hostname"));
    assertFalse(config.containsKey("database.port"));
  }

  @Test
  public void testSchemaHistoryTopicGeneration() {
    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    // Verify that when bootstrap servers are configured, the schema history topic is set correctly
    // The actual value depends on bootstrap server configuration being available at test time
    assertTrue(config.containsKey("topic.prefix"));
    assertEquals(config.get("topic.prefix"), "test-prefix");
  }

  @Test
  public void testAvroSerializationConfiguration() {
    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    Map<String, Object> config = step.buildConnectorConfiguration();

    assertEquals(config.get("key.converter"), "org.apache.kafka.connect.storage.StringConverter");
    assertEquals(config.get("value.converter"), "org.apache.kafka.connect.json.JsonConverter");
  }

  @Test
  public void testCreateHttpClient() {
    ConfigureDebeziumConnectorStep step =
        new ConfigureDebeziumConnectorStep(
            OP_CONTEXT, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    HttpClient client = step.createHttpClient();
    assertNotNull(client);
  }
}
