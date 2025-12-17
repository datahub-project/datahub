package com.linkedin.datahub.upgrade.system.kafka;

import static com.linkedin.gms.factory.kafka.common.AdminClientFactory.buildKafkaAdminClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.config.TopicBuilder;

@Slf4j
public class CreateKafkaTopicsStep implements UpgradeStep {

  private final OperationContext _opContext;
  private final KafkaConfiguration _kafkaConfiguration;
  private final KafkaProperties _kafkaProperties;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

      try {
        // Debug logging to understand configuration state
        log.info(
            "KafkaConfiguration setup: {}",
            OBJECT_MAPPER
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(_kafkaConfiguration.getSetup()));
        log.info(
            "KafkaConfiguration topics: {}",
            OBJECT_MAPPER
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(_kafkaConfiguration.getTopics()));
        log.info(
            "KafkaConfiguration topicDefaults: {}",
            OBJECT_MAPPER
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(_kafkaConfiguration.getTopicDefaults()));

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
        // and topics to increase partition count
        List<NewTopic> topicsToCreate = new ArrayList<>();
        List<String> topicsToSkip = new ArrayList<>();
        Map<String, NewPartitions> partitionsToIncrease = new HashMap<>();
        List<String> failedTopics = new ArrayList<>();

        for (Map.Entry<String, TopicsConfiguration.TopicConfiguration> entry :
            topicsConfig.getTopics().entrySet()) {
          String topicKey = entry.getKey();
          TopicsConfiguration.TopicConfiguration topicConfig = entry.getValue();

          // Skip if topic should not be created
          if (!topicConfig.getEnabled()) {
            log.debug("Skipping topic {} - create flag is false", topicKey);
            continue;
          }

          String topicName = topicConfig.getName();

          // Check if topic already exists
          if (existingTopics.contains(topicName)) {
            // For existing topics, check if partition count needs to be increased
            try {
              int currentPartitions = getCurrentPartitionCount(adminClient, topicName);
              int desiredPartitions = topicConfig.getPartitions();

              if (currentPartitions < desiredPartitions) {
                log.info(
                    "Topic {} exists with {} partitions, increasing to {}",
                    topicName,
                    currentPartitions,
                    desiredPartitions);
                partitionsToIncrease.put(topicName, NewPartitions.increaseTo(desiredPartitions));
              } else if (currentPartitions > desiredPartitions) {
                log.warn(
                    "Topic {} has {} partitions but configuration specifies {}. "
                        + "Kafka does not support reducing partition count - leaving unchanged",
                    topicName,
                    currentPartitions,
                    desiredPartitions);
                topicsToSkip.add(topicName);
              } else {
                log.info(
                    "Topic {} already has {} partitions - no change needed",
                    topicName,
                    currentPartitions);
                topicsToSkip.add(topicName);
              }
            } catch (Exception e) {
              log.error(
                  "Failed to check partition count for topic {}: {}", topicName, e.getMessage(), e);
              failedTopics.add(topicName);
            }
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
          log.info(
              "Skipping {} existing topics with no changes needed: {}",
              topicsToSkip.size(),
              topicsToSkip);
        }

        // Create new topics if any
        if (!topicsToCreate.isEmpty()) {
          log.info("Creating {} new topics in bulk", topicsToCreate.size());
          CreateTopicsResult createResult = adminClient.createTopics(topicsToCreate);
          createResult.all().get(); // Wait for all topics to be created
          log.info("Successfully created {} Kafka topics", topicsToCreate.size());
        }

        // Increase partitions for existing topics if needed
        if (!partitionsToIncrease.isEmpty()) {
          log.info(
              "Increasing partition count for {} topics: {}",
              partitionsToIncrease.size(),
              partitionsToIncrease.keySet());
          CreatePartitionsResult partitionsResult =
              adminClient.createPartitions(partitionsToIncrease);
          partitionsResult.all().get(); // Wait for all partition increases to complete
          log.info("Successfully increased partitions for {} topics", partitionsToIncrease.size());
        }

        if (topicsToCreate.isEmpty() && partitionsToIncrease.isEmpty()) {
          log.info(
              "All configured topics already exist with correct configuration - no changes needed");
        }

        // Check if any topics failed to be checked for partition count
        if (!failedTopics.isEmpty()) {
          String errorMessage =
              String.format(
                  "Failed to check partition count for %d topics: %s. "
                      + "These topics may not have the correct partition configuration.",
                  failedTopics.size(), failedTopics);
          log.error(errorMessage);
          throw new RuntimeException(errorMessage);
        }

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
      throw new RuntimeException("Failed to list existing topics: " + e.getMessage(), e);
    }
  }

  /**
   * Get the current partition count for a topic from Kafka.
   *
   * @param adminClient The Kafka AdminClient instance
   * @param topicName The name of the topic to check
   * @return The current number of partitions for the topic
   * @throws Exception if unable to retrieve topic description
   */
  private int getCurrentPartitionCount(AdminClient adminClient, String topicName) throws Exception {
    try {
      DescribeTopicsResult describeResult =
          adminClient.describeTopics(java.util.Collections.singletonList(topicName));
      TopicDescription topicDescription = describeResult.allTopicNames().get().get(topicName);
      return topicDescription.partitions().size();
    } catch (Exception e) {
      log.error("Failed to get partition count for topic {}: {}", topicName, e.getMessage(), e);
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
