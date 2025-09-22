package com.linkedin.datahub.upgrade.system.cdc.debezium;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.DebeziumConfiguration;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

/**
 * Step that configures and creates the Debezium connector for CDC processing. Builds the connector
 * configuration from available sources and posts it to Kafka Connect.
 */
@Slf4j
public class ConfigureDebeziumConnectorStep implements UpgradeStep {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final long REQUEST_TIMEOUT_MS = 30000;

  @SuppressWarnings("unused")
  private final OperationContext opContext;

  private final DebeziumConfiguration debeziumConfig;
  private final EbeanConfiguration ebeanConfig;
  private final KafkaConfiguration kafkaConfig;
  private final KafkaProperties kafkaProperties;

  public ConfigureDebeziumConnectorStep(
      OperationContext opContext,
      DebeziumConfiguration debeziumConfig,
      EbeanConfiguration ebeanConfig,
      KafkaConfiguration kafkaConfig,
      KafkaProperties kafkaProperties) {
    this.opContext = opContext;
    this.debeziumConfig = debeziumConfig;
    this.ebeanConfig = ebeanConfig;
    this.kafkaConfig = kafkaConfig;
    this.kafkaProperties = kafkaProperties;
  }

  @Override
  public String id() {
    return "ConfigureDebeziumConnectorStep";
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      log.info("Configuring Debezium connector for CDC processing...");

      try {
        String connectUrl = debeziumConfig.getUrl();
        if (connectUrl == null || connectUrl.isEmpty()) {
          log.error(
              "Kafka Connect URL not configured in CDC configuration - cannot configure connector");
          return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.FAILED);
        }

        Map<String, Object> connectorConfig = buildConnectorConfiguration();
        if (connectorConfig.isEmpty()) {
          log.error("Failed to build connector configuration");
          return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.FAILED);
        }
        HttpClient httpClient = createHttpClient();

        String connectorName = debeziumConfig.getName();
        if (connectorExists(httpClient, connectUrl, connectorName)) {
          return updateConnector(httpClient, connectUrl, connectorName, connectorConfig);
        } else {
          return createConnector(httpClient, connectUrl, connectorName, connectorConfig);
        }

      } catch (Exception e) {
        log.error("Failed to configure Debezium connector: {}", e.getMessage(), e);
        return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  /**
   * Builds connector configuration by merging database connection details, Kafka settings, and
   * explicit configuration overrides in that priority order.
   */
  protected Map<String, Object> buildConnectorConfiguration() {
    Map<String, Object> config = new HashMap<>();

    injectDatabaseConnection(config);
    injectKafkaConnection(config);

    if (debeziumConfig.getConfig() != null) {
      config.putAll(debeziumConfig.getConfig());
    }

    try {
      log.info(
          "Built connector configuration with {} properties",
          OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(config));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return config;
  }

  /** Extracts database connection from environment variables and EbeanConfiguration. */
  private void injectDatabaseConnection(Map<String, Object> config) {
    try {
      String hostPort = System.getenv("EBEAN_DATASOURCE_HOST");
      String dbName = System.getenv("DATAHUB_DB_NAME");

      if (hostPort != null && !hostPort.isEmpty()) {
        String[] hostPortParts = hostPort.split(":");
        if (hostPortParts.length == 0 || hostPortParts[0].trim().isEmpty()) {
          log.warn("Invalid EBEAN_DATASOURCE_HOST format: {}", hostPort);
          return;
        }

        config.put("database.hostname", hostPortParts[0].trim());

        if (hostPortParts.length > 1) {
          try {
            Integer.parseInt(hostPortParts[1].trim());
            config.put("database.port", hostPortParts[1].trim());
          } catch (NumberFormatException e) {
            log.warn("Invalid port number in EBEAN_DATASOURCE_HOST: {}", hostPortParts[1]);
          }
        } else {
          if (ebeanConfig.getUrl() != null && ebeanConfig.getUrl().contains("postgresql")) {
            config.put("database.port", "5432");
          } else if (ebeanConfig.getUrl() != null && ebeanConfig.getUrl().contains("mysql")) {
            config.put("database.port", "3306");
          }
        }
      } else {
        log.warn(
            "EBEAN_DATASOURCE_HOST environment variable not set - database connection may fail");
      }

      if (dbName != null && !dbName.isEmpty()) {
        config.put("database.include.list", dbName);
      } else {
        log.warn("DATAHUB_DB_NAME environment variable not set - CDC may capture all databases");
      }

      String cdcUser = System.getenv("CDC_USER");
      String cdcPassword = System.getenv("CDC_PASSWORD");

      if (cdcUser != null && !cdcUser.trim().isEmpty()) {
        config.put("database.user", cdcUser);
      } else if (ebeanConfig.getUsername() != null && !ebeanConfig.getUsername().trim().isEmpty()) {
        log.warn(
            "CDC_USER not set, falling back to regular database user - this may lack CDC privileges");
        config.put("database.user", ebeanConfig.getUsername());
      } else {
        log.warn("Neither CDC_USER nor database username configured - authentication may fail");
      }

      if (cdcPassword != null && !cdcPassword.trim().isEmpty()) {
        config.put("database.password", cdcPassword);
      } else if (ebeanConfig.getPassword() != null && !ebeanConfig.getPassword().trim().isEmpty()) {
        log.warn("CDC_PASSWORD not set, falling back to regular database password - this may fail");
        config.put("database.password", ebeanConfig.getPassword());
      } else {
        log.warn("Neither CDC_PASSWORD nor database password configured - authentication may fail");
      }

    } catch (Exception e) {
      log.error("Failed to configure database connection details: {}", e.getMessage(), e);
      throw new IllegalStateException(
          "Cannot proceed without database connection configuration", e);
    }
  }

  /** Configures Kafka connection, prioritizing kafkaConfiguration over kafkaProperties. */
  private void injectKafkaConnection(Map<String, Object> config) {
    String bootstrapServers = kafkaConfig.getBootstrapServers();

    if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
      var serversList = kafkaProperties.getBootstrapServers();
      if (serversList != null && !serversList.isEmpty()) {
        bootstrapServers = String.join(",", serversList);
      }
    }

    if (bootstrapServers != null && !bootstrapServers.trim().isEmpty()) {
      config.put("schema.history.internal.kafka.bootstrap.servers", bootstrapServers);

      String topicPrefix = (String) config.get("topic.prefix");
      if (topicPrefix != null) {
        config.put("schema.history.internal.kafka.topic", topicPrefix + ".schema-changes");
      }
    } else {
      log.warn(
          "No Kafka bootstrap servers configured in either kafkaConfiguration or kafkaProperties");
    }

    // Configure Avro serialization for CDC messages
    injectAvroSerialization(config);
  }

  /** Configures Avro serialization for CDC messages instead of JSON. */
  private void injectAvroSerialization(Map<String, Object> config) {
    // TODO: Use avro serialization
    // Configure key converter to use String and value converter to use Avro
    config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");

    /*config.put("value.converter", "io.apicurio.registry.utils.converter.AvroConverter");

    // Confluent schema registry compatibility mode
    config.put("converter.apicurio.registry.as-confluent", "true");
    config.put("converter.apicurio.use-id", "true");
    config.put("converter.schemas.enable", "false");
    config.put("converter.apicurio.registry.headers.enabled", "false");

    // Configure schema registry URL if available
    String schemaRegistryUrl = kafkaConfig.getSchemaRegistry().getUrl();
    if (schemaRegistryUrl != null && !schemaRegistryUrl.trim().isEmpty()) {
      // Only value converter needs schema registry (key is String)
      config.put("value.converter.schema.registry.url", schemaRegistryUrl);
      log.info("Configured String key converter and Avro value converter with schema registry: {}", schemaRegistryUrl);
    } else {
      log.error("Missing schema registry configuration");
      throw new IllegalStateException("Missing Schema registry config");
    }*/
  }

  /** Checks connector existence via Kafka Connect REST API. */
  private boolean connectorExists(HttpClient httpClient, String connectUrl, String connectorName)
      throws Exception {
    String url = connectUrl + "/connectors/" + connectorName;
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofMillis(REQUEST_TIMEOUT_MS))
            .GET()
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    return response.statusCode() == 200;
  }

  /** Creates connector via Kafka Connect REST API. */
  private UpgradeStepResult createConnector(
      HttpClient httpClient, String connectUrl, String connectorName, Map<String, Object> config)
      throws Exception {
    String url = connectUrl + "/connectors";
    Map<String, Object> requestBody =
        Map.of(
            "name", connectorName,
            "config", config);

    String jsonBody = OBJECT_MAPPER.writeValueAsString(requestBody);
    log.info(
        "Connector configuration: {}",
        OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(requestBody));

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofMillis(REQUEST_TIMEOUT_MS))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() == 201) {
      log.info("Successfully created Debezium connector '{}'", connectorName);
      return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.SUCCEEDED);
    } else {
      log.error(
          "Failed to create connector. Status: {}, Response: {}",
          response.statusCode(),
          response.body());
      return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.FAILED);
    }
  }

  /** Updates connector configuration via Kafka Connect REST API. */
  private UpgradeStepResult updateConnector(
      HttpClient httpClient, String connectUrl, String connectorName, Map<String, Object> config)
      throws Exception {
    String url = connectUrl + "/connectors/" + connectorName + "/config";
    String jsonBody = OBJECT_MAPPER.writeValueAsString(config);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofMillis(REQUEST_TIMEOUT_MS))
            .header("Content-Type", "application/json")
            .PUT(HttpRequest.BodyPublishers.ofString(jsonBody))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() == 200) {
      log.info("Successfully updated Debezium connector '{}'", connectorName);
      return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.SUCCEEDED);
    } else {
      log.error(
          "Failed to update connector. Status: {}, Response: {}",
          response.statusCode(),
          response.body());
      return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.FAILED);
    }
  }

  /**
   * Creates an HttpClient instance. This method is protected to allow mocking in unit tests.
   *
   * @return HttpClient instance configured for Kafka Connect REST API calls
   */
  protected HttpClient createHttpClient() {
    return HttpClient.newBuilder().connectTimeout(Duration.ofMillis(REQUEST_TIMEOUT_MS)).build();
  }
}
