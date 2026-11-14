package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;

/**
 * The optional topic that carries dataHubPolicy change logs to every process with a policy cache.
 * It is configured as {@code kafka.topics.policyChange}, and its {@code enabled} value follows
 * POLICY_CACHE_BROADCAST_ENABLED.
 */
public final class PolicyChangeTopic {

  public static final String CONFIG_KEY = "policyChange";

  private PolicyChangeTopic() {}

  /** The topic name when the policy change topic is enabled; empty otherwise. */
  @Nonnull
  public static Optional<String> enabledTopicName(@Nonnull ConfigurationProvider config) {
    if (config.getKafka() == null || config.getKafka().getTopics() == null) {
      return Optional.empty();
    }
    final Map<String, TopicsConfiguration.TopicConfiguration> topics =
        config.getKafka().getTopics().getTopics();
    final TopicsConfiguration.TopicConfiguration topic =
        topics == null ? null : topics.get(CONFIG_KEY);
    if (topic == null
        || !Boolean.TRUE.equals(topic.getEnabled())
        || StringUtils.isBlank(topic.getName())) {
      return Optional.empty();
    }
    return Optional.of(topic.getName());
  }
}
