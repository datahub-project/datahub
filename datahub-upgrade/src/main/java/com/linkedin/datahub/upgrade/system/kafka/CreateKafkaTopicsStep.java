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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.kafka.config.TopicBuilder;

@Slf4j
public class CreateKafkaTopicsStep implements UpgradeStep {

  private final OperationContext opContext;
  private final KafkaConfiguration kafkaConfiguration;
  private final KafkaProperties kafkaProperties;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public CreateKafkaTopicsStep(
      OperationContext opContext,
      KafkaConfiguration kafkaConfiguration,
      KafkaProperties kafkaProperties) {
    this.opContext = opContext;
    this.kafkaConfiguration = kafkaConfiguration;
    this.kafkaProperties = kafkaProperties;
  }

  @Override
  public String id() {
    return "CreateKafkaTopicsStep";
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      if (kafkaConfiguration.getSetup() == null) {
        log.warn("Kafka setup configuration is null - skipping topic creation");
        return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.SUCCEEDED);
      }

      if (!kafkaConfiguration.getSetup().isPreCreateTopics()) {
        log.info("Skipping Kafka topic creation as preCreateTopics is false");
        return new DefaultUpgradeStepResult(this.id(), DataHubUpgradeState.SUCCEEDED);
      }

      log.info("Creating/updating Kafka topics...");

      try {
        // Debug logging to understand configuration state
        log.info(
            "KafkaConfiguration setup: {}",
            OBJECT_MAPPER
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(kafkaConfiguration.getSetup()));
        log.info(
            "KafkaConfiguration topics: {}",
            OBJECT_MAPPER
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(kafkaConfiguration.getTopics()));
        log.info(
            "KafkaConfiguration topicDefaults: {}",
            OBJECT_MAPPER
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(kafkaConfiguration.getTopicDefaults()));

        // Create AdminClient using AdminClientFactory
        AdminClient adminClient = createAdminClient();

        // Get topic configurations
        TopicsConfiguration topicsConfig = kafkaConfiguration.getTopics();
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

        // Collect topics to create and partitions to increase
        List<NewTopic> topicsToCreate = new ArrayList<>();
        Map<String, NewPartitions> partitionsToIncrease = new HashMap<>();
        Map<ConfigResource, Map<String, String>> declaredByTopic = new HashMap<>();
        boolean reconcileConfigs = kafkaConfiguration.getSetup().isReconcileExistingTopicConfigs();
        List<String> failedTopics = new ArrayList<>();

        // Batch fetch partition counts for existing topics
        Map<String, Integer> currentPartitionCounts =
            fetchPartitionCountsForExistingTopics(
                adminClient, topicsConfig, existingTopics, failedTopics);

        // Process all configured topics
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
            // Check if auto-increase is enabled before checking/increasing partitions
            if (kafkaConfiguration.getSetup().isAutoIncreasePartitions()) {
              if (currentPartitionCounts.containsKey(topicName)) {
                int currentPartitions = currentPartitionCounts.get(topicName);
                int desiredPartitions = topicConfig.getPartitions();

                if (currentPartitions < desiredPartitions) {
                  log.info(
                      "Checking kafka topic {}: Increasing partitions from {} to {}",
                      topicName,
                      currentPartitions,
                      desiredPartitions);
                  partitionsToIncrease.put(topicName, NewPartitions.increaseTo(desiredPartitions));
                } else if (currentPartitions > desiredPartitions) {
                  log.error(
                      "Checking kafka topic {}: Has {} partitions but configuration specifies {}. "
                          + "Kafka does not support reducing partition count on a topic",
                      topicName,
                      currentPartitions,
                      desiredPartitions);
                } else {
                  log.info(
                      "Checking kafka topic {}: Already has correct partition count ({})",
                      topicName,
                      currentPartitions);
                }
              }
              // If not in currentPartitionCounts, it means the describe failed
              // and the topic was already added to failedTopics
            } else {
              log.info(
                  "Checking kafka topic {}: Skipping partition count check (auto-increase disabled)",
                  topicName);
            }
            // Reconcile configProperties on existing topic when explicitly enabled.
            // Declared properties are collected here and aligned against the broker
            // below via incrementalAlterConfigs (additive SET — only declared keys
            // are touched). Closes the gap where broker auto-created topics keep
            // broker-default retention (notably DataHubUpgradeHistory_v1 which
            // needs retention.ms=-1).
            if (reconcileConfigs
                && topicConfig.getConfigProperties() != null
                && !topicConfig.getConfigProperties().isEmpty()) {
              declaredByTopic.put(
                  new ConfigResource(ConfigResource.Type.TOPIC, topicName),
                  topicConfig.getConfigProperties());
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
              "Checking kafka topic {}: Creating with {} partitions and replication factor {}",
              topicName,
              topicConfig.getPartitions(),
              topicConfig.getReplicationFactor());
        }

        // Create new topics if any
        if (!topicsToCreate.isEmpty()) {
          log.info("Creating {} new topics", topicsToCreate.size());
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

        // Reconcile configProperties on existing topics if enabled
        if (!declaredByTopic.isEmpty()) {
          reconcileTopicConfigs(adminClient, declaredByTopic);
        }

        if (topicsToCreate.isEmpty()
            && partitionsToIncrease.isEmpty()
            && declaredByTopic.isEmpty()) {
          log.info(
              "All configured topics already exist with correct configuration - no changes needed");
        }

        // Check if any topics failed to be created or configured
        if (!failedTopics.isEmpty()) {
          String errorMessage =
              String.format(
                  "Failed to create or configure %d topics: %s. "
                      + "These topics may not exist or have incorrect configuration.",
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

  /**
   * Reconcile declared topic configProperties against the broker, applying only the keys whose
   * current value differs from what is declared in {@code application.yaml}.
   */
  private static void reconcileTopicConfigs(
      AdminClient adminClient, Map<ConfigResource, Map<String, String>> declaredByTopic)
      throws Exception {
    log.info(
        "Reconciling configProperties on {} existing topics: {}",
        declaredByTopic.size(),
        declaredByTopic.keySet().stream().map(ConfigResource::name).sorted().toList());

    Map<ConfigResource, Map<String, String>> currentByTopic =
        fetchCurrentTopicConfigs(adminClient, declaredByTopic.keySet());

    Map<ConfigResource, Collection<AlterConfigOp>> toApply = new HashMap<>();
    int totalAdded = 0;
    int totalChanged = 0;
    for (Map.Entry<ConfigResource, Map<String, String>> entry : declaredByTopic.entrySet()) {
      ConfigResource topic = entry.getKey();
      TopicConfigDiff diff =
          diffConfigs(entry.getValue(), currentByTopic.getOrDefault(topic, Map.of()));
      if (diff.isEmpty()) {
        log.info(
            "Topic {}: declared properties already match broker - nothing to alter", topic.name());
      } else {
        log.info("Topic {}: added={} changed={}", topic.name(), diff.added(), diff.changed());
        toApply.put(topic, diff.ops());
        totalAdded += diff.added().size();
        totalChanged += diff.changed().size();
      }
    }

    if (toApply.isEmpty()) {
      log.info(
          "All declared properties already aligned with broker - no AlterConfigs request issued");
      return;
    }
    adminClient.incrementalAlterConfigs(toApply).all().get();
    log.info(
        "Successfully applied configProperty changes on {} topics ({} added, {} changed)",
        toApply.size(),
        totalAdded,
        totalChanged);
  }

  /**
   * Read current broker-side topic configs for the given resources, as a flat key->value map per
   * topic.
   */
  private static Map<ConfigResource, Map<String, String>> fetchCurrentTopicConfigs(
      AdminClient adminClient, Collection<ConfigResource> resources) throws Exception {
    Map<ConfigResource, Config> raw = adminClient.describeConfigs(resources).all().get();
    Map<ConfigResource, Map<String, String>> out = new HashMap<>(raw.size());
    raw.forEach(
        (res, cfg) -> {
          Map<String, String> entries = new HashMap<>();
          cfg.entries().forEach(ce -> entries.put(ce.name(), ce.value()));
          out.put(res, entries);
        });
    return out;
  }

  /** Compute the diff between declared (yaml) and current (broker) topic config maps. */
  private static TopicConfigDiff diffConfigs(
      Map<String, String> declared, Map<String, String> current) {
    List<String> added = new ArrayList<>();
    List<String> changed = new ArrayList<>();
    List<AlterConfigOp> ops = new ArrayList<>();
    declared.forEach(
        (key, desired) -> {
          String live = current.get(key);
          if (live == null) {
            added.add("+ " + key + "=" + desired);
            ops.add(new AlterConfigOp(new ConfigEntry(key, desired), AlterConfigOp.OpType.SET));
          } else if (!live.equals(desired)) {
            changed.add("~ " + key + ": " + live + " -> " + desired);
            ops.add(new AlterConfigOp(new ConfigEntry(key, desired), AlterConfigOp.OpType.SET));
          }
        });
    Collections.sort(added);
    Collections.sort(changed);
    return new TopicConfigDiff(added, changed, ops);
  }

  private record TopicConfigDiff(
      List<String> added, List<String> changed, List<AlterConfigOp> ops) {
    boolean isEmpty() {
      return ops.isEmpty();
    }
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
   * Fetches current partition counts for all existing configured topics
   *
   * @param adminClient Kafka AdminClient instance
   * @param topicsConfig Configuration containing all topic definitions
   * @param existingTopics Set of topics that already exist in Kafka
   * @param failedTopics List to collect topics that failed partition count checks
   * @return Map of topic names to their current partition counts
   */
  private Map<String, Integer> fetchPartitionCountsForExistingTopics(
      AdminClient adminClient,
      TopicsConfiguration topicsConfig,
      Set<String> existingTopics,
      List<String> failedTopics) {
    // Collect all existing topics that are in our configuration
    List<String> existingConfiguredTopics = new ArrayList<>();
    for (Map.Entry<String, TopicsConfiguration.TopicConfiguration> entry :
        topicsConfig.getTopics().entrySet()) {
      TopicsConfiguration.TopicConfiguration topicConfig = entry.getValue();
      if (topicConfig.getEnabled() && existingTopics.contains(topicConfig.getName())) {
        existingConfiguredTopics.add(topicConfig.getName());
      }
    }

    // Batch API call to get partition counts for all existing topics
    Map<String, Integer> currentPartitionCounts = new HashMap<>();
    if (!existingConfiguredTopics.isEmpty()
        && kafkaConfiguration.getSetup().isAutoIncreasePartitions()) {
      try {
        DescribeTopicsResult describeResult = adminClient.describeTopics(existingConfiguredTopics);
        Map<String, TopicDescription> topicDescriptions = describeResult.allTopicNames().get();
        for (Map.Entry<String, TopicDescription> descEntry : topicDescriptions.entrySet()) {
          currentPartitionCounts.put(descEntry.getKey(), descEntry.getValue().partitions().size());
        }
        log.info(
            "Retrieved partition counts for {} existing topics", currentPartitionCounts.size());
      } catch (Exception e) {
        log.error(
            "Failed to describe topics: {}. Will skip partition checks for existing topics.",
            e.getMessage(),
            e);
        failedTopics.addAll(existingConfiguredTopics);
      }
    }
    return currentPartitionCounts;
  }

  /**
   * Creates an AdminClient instance using the AdminClientFactory. This method is extracted to allow
   * for mocking in unit tests.
   *
   * @return AdminClient instance configured with the current Kafka configuration
   */
  protected AdminClient createAdminClient() {
    return buildKafkaAdminClient(
        kafkaConfiguration, kafkaProperties, "datahub-upgrade-kafka-setup");
  }
}
