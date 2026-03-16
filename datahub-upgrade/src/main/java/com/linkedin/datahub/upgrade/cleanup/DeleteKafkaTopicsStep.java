package com.linkedin.datahub.upgrade.cleanup;

import static com.linkedin.gms.factory.kafka.common.AdminClientFactory.buildKafkaAdminClient;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

/** Deletes all Kafka topics that DataHub created during setup. */
@Slf4j
public class DeleteKafkaTopicsStep implements UpgradeStep {

  private final KafkaConfiguration kafkaConfiguration;
  private final KafkaProperties kafkaProperties;

  public DeleteKafkaTopicsStep(
      KafkaConfiguration kafkaConfiguration, KafkaProperties kafkaProperties) {
    this.kafkaConfiguration = kafkaConfiguration;
    this.kafkaProperties = kafkaProperties;
  }

  @Override
  public String id() {
    return "DeleteKafkaTopicsStep";
  }

  @Override
  public int retryCount() {
    return 2;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      TopicsConfiguration topicsConfig = kafkaConfiguration.getTopics();
      if (topicsConfig == null
          || topicsConfig.getTopics() == null
          || topicsConfig.getTopics().isEmpty()) {
        log.info("No topics configured — nothing to delete");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      }

      List<String> topicNames = new ArrayList<>();
      for (Map.Entry<String, TopicsConfiguration.TopicConfiguration> entry :
          topicsConfig.getTopics().entrySet()) {
        TopicsConfiguration.TopicConfiguration topicConfig = entry.getValue();
        if (topicConfig.getEnabled()) {
          topicNames.add(topicConfig.getName());
        }
      }

      if (topicNames.isEmpty()) {
        log.info("All configured topics are disabled — nothing to delete");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      }

      log.info("Deleting {} Kafka topics: {}", topicNames.size(), topicNames);

      try (AdminClient adminClient = createAdminClient()) {
        adminClient.deleteTopics(topicNames).all().get();
        log.info("Successfully deleted {} Kafka topics", topicNames.size());
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("Failed to delete Kafka topics: {}", e.getMessage(), e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  protected AdminClient createAdminClient() {
    return buildKafkaAdminClient(kafkaConfiguration, kafkaProperties, "datahub-cleanup-kafka");
  }
}
