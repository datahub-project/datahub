package com.linkedin.metadata.kafka.listener;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.kafka.config.ContainerPostProcessor;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.converter.MessageConverter;

/**
 * A {@link KafkaListenerEndpoint} that directly sets a {@link BatchMessageListener} on the
 * container, bypassing Spring's {@code BatchMessagingMessageListenerAdapter}. This avoids the
 * parameter-resolution issues that occur when {@code MethodKafkaListenerEndpoint} tries to invoke a
 * method accepting {@code List<ConsumerRecord>} in batch mode — the adapter strips {@code
 * ConsumerRecord} wrappers before method invocation, causing a {@code
 * MethodArgumentResolutionException}.
 */
public class BatchKafkaListenerEndpoint<K, V> implements KafkaListenerEndpoint {

  private final String id;
  private final String groupId;
  private final List<String> topics;
  private final BatchMessageListener<K, V> messageListener;

  public BatchKafkaListenerEndpoint(
      @Nonnull String id,
      @Nonnull String groupId,
      @Nonnull List<String> topics,
      @Nonnull BatchMessageListener<K, V> messageListener) {
    this.id = id;
    this.groupId = groupId;
    this.topics = topics;
    this.messageListener = messageListener;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getGroupId() {
    return groupId;
  }

  @Override
  public String getGroup() {
    return null;
  }

  @Override
  public Collection<String> getTopics() {
    return topics;
  }

  @Override
  public TopicPartitionOffset[] getTopicPartitionsToAssign() {
    return null;
  }

  @Override
  public Pattern getTopicPattern() {
    return null;
  }

  @Override
  public String getClientIdPrefix() {
    return null;
  }

  @Override
  public Integer getConcurrency() {
    return null;
  }

  @Override
  public Boolean getAutoStartup() {
    return false;
  }

  @Override
  public void setupListenerContainer(
      MessageListenerContainer listenerContainer, @Nullable MessageConverter messageConverter) {
    listenerContainer.setupMessageListener(messageListener);
  }

  @Override
  public boolean isSplitIterables() {
    return false;
  }

  @Override
  public Properties getConsumerProperties() {
    return new Properties();
  }

  @Override
  public Boolean getBatchListener() {
    return true;
  }

  @Override
  public ContainerPostProcessor<?, ?, ?> getContainerPostProcessor() {
    return null;
  }
}
