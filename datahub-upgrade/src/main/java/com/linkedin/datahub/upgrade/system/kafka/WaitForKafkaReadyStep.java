package com.linkedin.datahub.upgrade.system.kafka;

import static com.linkedin.gms.factory.kafka.common.AdminClientFactory.buildKafkaAdminClient;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

@Slf4j
public class WaitForKafkaReadyStep implements UpgradeStep {

  // Constants for timeout and retry configuration
  private static final int RETRY_COUNT = 60;
  private static final long RETRY_INTERVAL_SECONDS = 5;
  private static final long REQUEST_TIMEOUT_MS = 5000;

  private final OperationContext _opContext;
  private final KafkaConfiguration _kafkaConfiguration;
  private final KafkaProperties _kafkaProperties;

  public WaitForKafkaReadyStep(
      OperationContext opContext,
      KafkaConfiguration kafkaConfiguration,
      KafkaProperties kafkaProperties) {
    this._opContext = opContext;
    this._kafkaConfiguration = kafkaConfiguration;
    this._kafkaProperties = kafkaProperties;
  }

  @Override
  public String id() {
    return "WaitForKafkaReadyStep";
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      log.info("Waiting for Kafka to be ready...");

      try {
        // Create AdminClient using the extracted method
        AdminClient adminClient = createAdminClient();

        // Get cluster information to check all brokers are reachable
        var clusterResult = adminClient.describeCluster();
        var clusterId = clusterResult.clusterId().get(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        var nodes = clusterResult.nodes().get(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        // Verify we have brokers available
        if (nodes.isEmpty()) {
          throw new RuntimeException("No brokers found in Kafka cluster");
        }

        log.debug("Found {} brokers in Kafka cluster", nodes.size());

        log.info("Kafka cluster is ready - {} brokers are reachable and healthy", nodes.size());
        return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.SUCCEEDED);

      } catch (Exception e) {
        // Retries is done by UpgradeManager based on retryCount
        try {
          Thread.sleep(RETRY_INTERVAL_SECONDS * 1000);
        } catch (InterruptedException ie) {
          log.error("Interrupted while waiting for Kafka to be ready", ie);
          return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.FAILED);
        }

        return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  protected AdminClient createAdminClient() {
    return buildKafkaAdminClient(
        _kafkaConfiguration, _kafkaProperties, "datahub-upgrade-kafka-setup");
  }

  @Override
  public int retryCount() {
    return RETRY_COUNT;
  }
}
