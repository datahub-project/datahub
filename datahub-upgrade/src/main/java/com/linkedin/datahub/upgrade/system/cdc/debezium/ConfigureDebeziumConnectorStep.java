package com.linkedin.datahub.upgrade.system.cdc.debezium;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.codehaus.plexus.util.StringUtils;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

/**
 * Step that configures and creates the Debezium connector for CDC processing. Builds the connector
 * configuration from available sources and posts it to Kafka Connect.
 */
@Slf4j
public class ConfigureDebeziumConnectorStep implements UpgradeStep {

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
  @com.google.common.annotations.VisibleForTesting
  Map<String, Object> buildConnectorConfiguration() {
    Map<String, Object> config = new HashMap<>();

    injectDatabaseConnection(config);
    injectKafkaConnection(config);

    if (debeziumConfig.getConfig() != null) {
      config.putAll(debeziumConfig.getConfig());
    }

    try {
      log.info(
          "Built connector configuration with {} properties",
          opContext.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(config));
    } catch (JsonProcessingException e) {
      log.warn(
          "Unable to process config as json with error {} for config {}", e.getMessage(), config);
    }

    return config;
  }

  /** Extracts database connection from environment variables and EbeanConfiguration. */
  private void injectDatabaseConnection(Map<String, Object> config) {
    String dbConnectionUrl = ebeanConfig.getUrl();

    // Parse JDBC URL to extract hostname, port, and database name
    if (dbConnectionUrl != null && !dbConnectionUrl.isEmpty()) {
      String urlPattern = "jdbc:([^:]+)://([^:/]+)(?::(\\d+))?/([^?]+)";
      java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(urlPattern);
      java.util.regex.Matcher matcher = pattern.matcher(dbConnectionUrl);

      if (matcher.find()) {
        String dbType = matcher.group(1);
        String hostname = matcher.group(2);
        String portStr = matcher.group(3);
        String database = matcher.group(4);

        config.put("database.hostname", hostname);

        // Set default port if not specified
        if (portStr != null) {
          config.put("database.port", portStr);
        } else {
          // Use default ports
          String defaultPort = "mysql".equals(dbType) ? "3306" : "5432";
          config.put("database.port", defaultPort);
        }

        log.info(
            "Parsed database connection: host={}, port={}, database={}",
            hostname,
            config.get("database.port"),
            database);
      } else {
        log.warn(
            "Could not parse database URL format: {}, falling back to mclProcessing.cdcSource.debeziumConfig.config",
            dbConnectionUrl);
      }
    }
  }

  /** Configures Kafka connection, prioritizing kafkaConfiguration over kafkaProperties. */
  private void injectKafkaConnection(Map<String, Object> config) {
    // Configures the producer for the Debezium connector, so override with producer config if
    // present
    String bootstrapServers =
        StringUtils.isNotBlank(kafkaConfig.getProducer().getBootstrapServers())
            ? kafkaConfig.getProducer().getBootstrapServers()
            : kafkaConfig.getBootstrapServers();

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

    String jsonBody = opContext.getObjectMapper().writeValueAsString(requestBody);
    log.info(
        "Connector configuration: {}",
        opContext
            .getObjectMapper()
            .writerWithDefaultPrettyPrinter()
            .writeValueAsString(requestBody));

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
    String jsonBody = opContext.getObjectMapper().writeValueAsString(config);

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
