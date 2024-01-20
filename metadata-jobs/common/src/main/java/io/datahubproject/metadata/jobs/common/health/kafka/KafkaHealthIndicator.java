package io.datahubproject.metadata.jobs.common.health.kafka;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.boot.kafka.DataHubUpgradeKafkaListener;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaHealthIndicator extends AbstractHealthIndicator {

  private final KafkaListenerEndpointRegistry listenerRegistry;
  private final ConfigurationProvider configurationProvider;

  public KafkaHealthIndicator(
      KafkaListenerEndpointRegistry listenerRegistry, ConfigurationProvider configurationProvider) {
    this.listenerRegistry = listenerRegistry;
    this.configurationProvider = configurationProvider;
  }

  @Override
  protected void doHealthCheck(Health.Builder builder) throws Exception {
    Status kafkaStatus = Status.UP;
    boolean isContainerDown =
        listenerRegistry.getAllListenerContainers().stream()
            .filter(
                container ->
                    !DataHubUpgradeKafkaListener.CONSUMER_GROUP.equals(container.getGroupId()))
            .anyMatch(container -> !container.isRunning());
    Map<String, ConsumerDetails> details =
        listenerRegistry.getAllListenerContainers().stream()
            .collect(
                Collectors.toMap(
                    MessageListenerContainer::getListenerId, this::buildConsumerDetails));
    if (isContainerDown && configurationProvider.getKafka().getConsumer().isHealthCheckEnabled()) {
      kafkaStatus = Status.DOWN;
    }
    builder.status(kafkaStatus).withDetails(details).build();
  }

  private ConsumerDetails buildConsumerDetails(MessageListenerContainer container) {
    Collection<TopicPartition> partitionDetails = container.getAssignedPartitions();
    int concurrency = 1;
    if (container
        instanceof ConcurrentMessageListenerContainer<?, ?> concurrentMessageListenerContainer) {
      concurrency = concurrentMessageListenerContainer.getConcurrency();
    }
    return new ConsumerDetails(
        partitionDetails,
        container.getListenerId(),
        container.getGroupId(),
        concurrency,
        container.isRunning());
  }

  @Value
  private static class ConsumerDetails {
    Collection<TopicPartition> partitionDetails;
    String listenerId;
    String groupId;
    int concurrency;
    boolean isRunning;
  }
}
