package com.linkedin.metadata.config.kafka;

import lombok.Data;

@Data
public class SetupConfiguration {
  private boolean preCreateTopics = true;
  private boolean useConfluentSchemaRegistry = true;
  private boolean autoIncreasePartitions = true;
}
