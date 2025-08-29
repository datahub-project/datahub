package com.linkedin.datahub.upgrade.system.kafka;

import static com.linkedin.gms.factory.kafka.common.AdminClientFactory.buildKafkaAdminClient;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

@Slf4j
public class ConfluentSchemaRegistryCleanupPolicyStep implements UpgradeStep {

  private final OperationContext _opContext;
  private final KafkaConfiguration _kafkaConfiguration;
  private final KafkaProperties _kafkaProperties;

  public ConfluentSchemaRegistryCleanupPolicyStep(
      OperationContext opContext,
      KafkaConfiguration kafkaConfiguration,
      KafkaProperties kafkaProperties) {
    this._opContext = opContext;
    this._kafkaConfiguration = kafkaConfiguration;
    this._kafkaProperties = kafkaProperties;
  }

  @Override
  public String id() {
    return "ConfluentSchemaRegistryCleanupPolicyStep";
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      log.info("Configuring Confluent Schema Registry cleanup policies...");

      // Check if Confluent Schema Registry is enabled
      if (_kafkaConfiguration.getSetup() == null
          || !_kafkaConfiguration.getSetup().isUseConfluentSchemaRegistry()) {
        log.info(
            "Skipping Confluent Schema Registry cleanup policy configuration - schema registry is disabled");
        return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.SUCCEEDED);
      }

      try {
        // Create AdminClient using AdminClientFactory
        AdminClient adminClient = createAdminClient();

        // Configure cleanup policy for _schemas topic (equivalent to kafka-configs.sh)
        String schemasTopic = "_schemas";
        log.info("Configuring cleanup policy for schema registry topic: {}", schemasTopic);

        // Create config resource for the _schemas topic
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, schemasTopic);

        // Create the cleanup policy configuration
        ConfigEntry cleanupPolicyEntry = new ConfigEntry("cleanup.policy", "compact");

        // Create alter config operation
        AlterConfigOp alterConfigOp =
            new AlterConfigOp(cleanupPolicyEntry, AlterConfigOp.OpType.SET);
        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
        configs.put(configResource, Collections.singletonList(alterConfigOp));

        // Apply the configuration change
        log.info(
            "Applying cleanup policy configuration: topic={}, cleanup.policy=compact",
            schemasTopic);
        adminClient.incrementalAlterConfigs(configs).all().get();

        log.info(
            "Successfully configured cleanup policy for schema registry topic: {}", schemasTopic);
        return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.SUCCEEDED);

      } catch (Exception e) {
        log.error(
            "Failed to configure Confluent Schema Registry cleanup policies: {}",
            e.getMessage(),
            e);
        return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  /**
   * Creates an AdminClient instance using the AdminClientFactory. This method is extracted to allow
   * for mocking in unit tests.
   *
   * @return AdminClient instance configured with the current Kafka configuration
   */
  protected AdminClient createAdminClient() {
    return buildKafkaAdminClient(
        _kafkaConfiguration, _kafkaProperties, "datahub-upgrade-kafka-setup");
  }
}
