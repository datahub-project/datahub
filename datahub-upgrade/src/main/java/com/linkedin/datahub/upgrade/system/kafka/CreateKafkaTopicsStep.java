package com.linkedin.datahub.upgrade.system.kafka;

import static com.linkedin.gms.factory.kafka.common.AdminClientFactory.buildKafkaAdminClient;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.config.TopicBuilder;

@Slf4j
public class CreateKafkaTopicsStep implements UpgradeStep {

  private final OperationContext _opContext;
  private final KafkaConfiguration _kafkaConfiguration;
  private final KafkaProperties _kafkaProperties;

  public CreateKafkaTopicsStep(
      OperationContext opContext,
      KafkaConfiguration kafkaConfiguration,
      KafkaProperties kafkaProperties) {
    this._opContext = opContext;
    this._kafkaConfiguration = kafkaConfiguration;
    this._kafkaProperties = kafkaProperties;
  }

  @Override
  public String id() {
    return "CreateKafkaTopicsStep";
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      if (_kafkaConfiguration.getSetup() == null) {
        log.warn("Setup configuration is null - skipping topic creation");
        return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.SUCCEEDED);
      }

      if (!_kafkaConfiguration.getSetup().isPreCreateTopics()) {
        log.info("Skipping Kafka topic creation as preCreateTopics is false");
        return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.SUCCEEDED);
      }

      log.info("Creating Kafka topics...");

      // Debug logging to understand configuration state
      log.info("KafkaConfiguration setup: {}", _kafkaConfiguration.getSetup());
      log.info("KafkaConfiguration topics: {}", _kafkaConfiguration.getTopics());
      log.info("KafkaConfiguration topicDefaults: {}", _kafkaConfiguration.getTopicDefaults());

      try {
        // Create AdminClient using AdminClientFactory
        AdminClient adminClient = createAdminClient();

        // Get topic configurations
        TopicsConfiguration topicsConfig = _kafkaConfiguration.getTopics();
        if (topicsConfig == null) {
          log.warn("Topics configuration is null - skipping topic creation");
          return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.SUCCEEDED);
        }

        if (topicsConfig.getTopics() == null || topicsConfig.getTopics().isEmpty()) {
          log.warn("No topics configured for creation");
          // We dont really support a scenario of no topics to create with preCreateTopics set to
          // true
          return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.FAILED);
        }

        // Get existing topics to implement if-not-exists functionality
        Set<String> existingTopics = getExistingTopics(adminClient);
        log.info("Found {} existing topics: {}", existingTopics.size(), existingTopics);

        // Collect all topics to create (only those that don't exist)
        List<NewTopic> topicsToCreate = new ArrayList<>();
        List<String> topicsToSkip = new ArrayList<>();

        for (Map.Entry<String, TopicsConfiguration.TopicConfiguration> entry :
            topicsConfig.getTopics().entrySet()) {
          String topicKey = entry.getKey();
          TopicsConfiguration.TopicConfiguration topicConfig = entry.getValue();

          // Skip if topic should not be created
          if (!topicConfig.getCreate()) {
            log.debug("Skipping topic {} - create flag is false", topicKey);
            continue;
          }

          String topicName = topicConfig.getName();

          // Check if topic already exists
          if (existingTopics.contains(topicName)) {
            log.info("Topic {} already exists - skipping creation", topicName);
            topicsToSkip.add(topicName);
            continue;
          }

          // Use Spring's TopicBuilder to create the topic
          TopicBuilder topicBuilder =
              TopicBuilder.name(topicName)
                  .partitions(topicConfig.getPartitions())
                  .replicas(topicConfig.getReplicationFactor());

          // Set topic-specific configurations if provided
          if (topicConfig.getConfigProperties() != null
              && !topicConfig.getConfigProperties().isEmpty()) {
            topicConfig.getConfigProperties().forEach(topicBuilder::config);
          }

          topicsToCreate.add(topicBuilder.build());
          log.info(
              "Preparing to create topic: {} with {} partitions and replication factor {}",
              topicName,
              topicConfig.getPartitions(),
              topicConfig.getReplicationFactor());
        }

        // Log summary of what will be created vs skipped
        if (!topicsToSkip.isEmpty()) {
          log.info("Skipping {} existing topics: {}", topicsToSkip.size(), topicsToSkip);
        }

        if (topicsToCreate.isEmpty()) {
          log.info("All configured topics already exist - nothing to create");
          return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.SUCCEEDED);
        }

        // Create all topics in bulk
        log.info("Creating {} new topics in bulk", topicsToCreate.size());
        CreateTopicsResult result = adminClient.createTopics(topicsToCreate);
        result.all().get(); // Wait for all topics to be created

        log.info("Successfully created {} Kafka topics", topicsToCreate.size());
        return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.SUCCEEDED);

      } catch (Exception e) {
        log.error("Failed to create Kafka topics: {}", e.getMessage(), e);
        return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  /** Get the set of existing topic names from Kafka */
  private Set<String> getExistingTopics(AdminClient adminClient) throws Exception {
    try {
      ListTopicsResult listTopicsResult = adminClient.listTopics();
      return listTopicsResult.names().get();
    } catch (Exception e) {
      log.error("Failed to list existing topics: {}", e.getMessage(), e);
      throw e;
    }
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
