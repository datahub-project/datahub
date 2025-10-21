package com.linkedin.datahub.upgrade.system.cdc.debezium;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.DebeziumConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

/**
 * Step that waits for Kafka Connect (Debezium) to be ready and available. Makes HTTP requests to
 * the Kafka Connect REST API to verify connectivity.
 */
@Slf4j
public class WaitForDebeziumReadyStep implements UpgradeStep {

  // Debezium startup can take time, especially in containerized environments
  private static final int RETRY_COUNT = 20;
  private static final long RETRY_INTERVAL_SECONDS = 5;

  @SuppressWarnings("unused")
  private final OperationContext opContext; // Reserved for future audit logging

  private final DebeziumConfiguration debeziumConfig;

  @SuppressWarnings("unused")
  private final KafkaConfiguration
      kafkaConfiguration; // May be used for advanced Kafka connectivity checks

  @SuppressWarnings("unused")
  private final KafkaProperties
      kafkaProperties; // May be used for advanced Kafka connectivity checks

  public WaitForDebeziumReadyStep(
      OperationContext opContext,
      DebeziumConfiguration debeziumConfig,
      KafkaConfiguration kafkaConfiguration,
      KafkaProperties kafkaProperties) {
    this.opContext = opContext;
    this.debeziumConfig = debeziumConfig;
    this.kafkaConfiguration = kafkaConfiguration;
    this.kafkaProperties = kafkaProperties;
  }

  @Override
  public String id() {
    return "WaitForDebeziumReadyStep";
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      log.info("Waiting for Kafka Connect (Debezium) to be ready...");

      try {
        String connectUrl = getKafkaConnectUrl();
        if (connectUrl == null || connectUrl.isEmpty()) {
          log.warn("Kafka Connect URL not configured, skipping connectivity check");
          return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.SUCCEEDED);
        }

        log.info("Attempting to connect to Kafka Connect at: {}", connectUrl);

        String connectorsUrl = connectUrl + "/connectors";

        HttpClient httpClient = createHttpClient();

        HttpRequest request =
            HttpRequest.newBuilder()
                .uri(URI.create(connectorsUrl))
                .timeout(Duration.ofMillis(debeziumConfig.getRequestTimeoutMillis()))
                .GET()
                .build();

        HttpResponse<String> response =
            httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 200) {
          log.info("Kafka Connect is ready and responding at: {}", connectorsUrl);
          return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.SUCCEEDED);
        } else {
          log.warn("Kafka Connect returned status {}: {}", response.statusCode(), response.body());
          return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.FAILED);
        }

      } catch (Exception e) {
        String connectUrl = getKafkaConnectUrl();
        log.warn(
            "Failed to connect to Kafka Connect at '{}': {}. Will retry in {} seconds.",
            connectUrl,
            e.getMessage(),
            RETRY_INTERVAL_SECONDS);

        // UpgradeManager handles retries based on retryCount()
        try {
          Thread.sleep(RETRY_INTERVAL_SECONDS * 1000);
        } catch (InterruptedException ie) {
          log.error("Interrupted while waiting for Kafka Connect", ie);
          Thread.currentThread().interrupt(); // Restore interrupt status
          return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.FAILED);
        }

        return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  /**
   * Creates an HttpClient instance. This method is protected to allow mocking in unit tests.
   *
   * @return HttpClient instance configured for Kafka Connect REST API calls
   */
  protected HttpClient createHttpClient() {
    return HttpClient.newBuilder()
        .connectTimeout(Duration.ofMillis(debeziumConfig.getRequestTimeoutMillis()))
        .build();
  }

  /** Retrieves Kafka Connect REST endpoint from configuration. */
  private String getKafkaConnectUrl() {
    return debeziumConfig.getUrl();
  }

  @Override
  public int retryCount() {
    return RETRY_COUNT;
  }
}
