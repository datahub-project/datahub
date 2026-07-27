package com.linkedin.metadata.kafka;

import com.linkedin.metadata.kafka.pause.ConsumerPauseSupport;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerPauseSupport implements ConsumerPauseSupport {

  private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

  @Override
  public void pause(@Nonnull String listenerContainerId) {
    Optional<MessageListenerContainer> container =
        Optional.ofNullable(
            kafkaListenerEndpointRegistry.getListenerContainer(listenerContainerId));
    if (container.isEmpty()) {
      log.warn(
          "Expected Kafka listener container '{}' was missing; throttle pause skipped.",
          listenerContainerId);
    } else {
      container.ifPresent(MessageListenerContainer::pause);
    }
  }

  @Override
  public void resume(@Nonnull String listenerContainerId) {
    Optional<MessageListenerContainer> container =
        Optional.ofNullable(
            kafkaListenerEndpointRegistry.getListenerContainer(listenerContainerId));
    container.ifPresent(MessageListenerContainer::resume);
  }
}
