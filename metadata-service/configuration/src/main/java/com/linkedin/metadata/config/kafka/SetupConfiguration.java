package com.linkedin.metadata.config.kafka;

import lombok.Data;

@Data
public class SetupConfiguration {
  private boolean preCreateTopics = true;
  private boolean useConfluentSchemaRegistry = true;
  private boolean autoIncreasePartitions = false;
  // Opt-in: when true, CreateKafkaTopicsStep will alter declared configProperties on
  // already-existing topics (e.g. retention.ms on DataHubUpgradeHistory_v1) to match
  // application.yaml. Default is false because many deployments lack ALTER_CONFIGS
  // permissions on managed Kafka. See datahub-project/datahub#7882.
  private boolean reconcileExistingTopicConfigs = false;
}
