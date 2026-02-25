package com.linkedin.metadata.config.kafka;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TopicsConfiguration {
  // For backward compatibility
  private String dataHubUsage = "DataHubUsage_v1";

  private Map<String, TopicConfiguration> topics = new HashMap<>();

  private TopicConfiguration topicDefaults;

  // For backward compatibility
  public String getDataHubUsage() {
    if (topics != null && topics.containsKey("datahubUsageEvent")) {
      TopicConfiguration topicDef = topics.get("datahubUsageEvent");
      if (topicDef != null && topicDef.getName() != null) {
        return topicDef.getName();
      }
    }
    return dataHubUsage;
  }

  public TopicsConfiguration(
      TopicConfiguration topicDefaults, Map<String, TopicConfiguration> topics) {
    this.topics = topics;
    this.topicDefaults = topicDefaults;

    // Initialize null values in topics map with defaults from topicDefaults
    if (topics != null && topicDefaults != null) {
      for (TopicConfiguration topicConfig : topics.values()) {
        if (topicConfig != null) {
          // Initialize partitions if null
          if (topicConfig.getPartitions() == null) {
            topicConfig.setPartitions(topicDefaults.getPartitions());
          }

          // Initialize replicationFactor if null
          if (topicConfig.getReplicationFactor() == null) {
            topicConfig.setReplicationFactor(topicDefaults.getReplicationFactor());
          }

          // Initialize create if null
          if (topicConfig.getEnabled() == null) {
            topicConfig.setEnabled(topicDefaults.getEnabled());
          }

          // Initialize configProperties if null
          if (topicConfig.getConfigProperties() == null) {
            topicConfig.setConfigProperties(topicDefaults.getConfigProperties());
          } else if (topicDefaults.getConfigProperties() != null) {
            // Merge configProperties, keeping existing values as overrides
            Map<String, String> mergedConfig = new HashMap<>(topicDefaults.getConfigProperties());
            mergedConfig.putAll(topicConfig.getConfigProperties());
            topicConfig.setConfigProperties(mergedConfig);
          }
        }
      }
    }
  }

  @Getter
  @Setter
  public static class TopicConfiguration {
    private String name;
    private Integer partitions;
    private Integer replicationFactor;
    private Map<String, String> configProperties;
    private Boolean enabled = true; // Default to true if not specified
  }
}
