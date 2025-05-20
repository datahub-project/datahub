package com.linkedin.metadata.kafka.generic;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;

@Slf4j
public abstract class AbstractKafkaListenerRegistrar<E, H extends EventHook<E>>
    implements GenericKafkaListenerRegistrar<E, H>, InitializingBean {

  protected KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
  protected KafkaListenerContainerFactory<?> kafkaListenerContainerFactory;
  protected String consumerGroupBase;
  protected List<H> hooks;
  protected ObjectMapper objectMapper;

  @Override
  public void afterPropertiesSet() {
    Map<String, List<H>> hookGroups =
        getEnabledHooks().stream().collect(Collectors.groupingBy(H::getConsumerGroupSuffix));

    log.info(
        "{} Consumer Groups: {}",
        getProcessorType(),
        hookGroups.keySet().stream().map(this::buildConsumerGroupName).collect(Collectors.toSet()));

    hookGroups.forEach(
        (key, groupHooks) -> {
          KafkaListenerEndpoint kafkaListenerEndpoint =
              createListenerEndpoint(buildConsumerGroupName(key), getTopicNames(), groupHooks);
          registerKafkaListener(kafkaListenerEndpoint, false);
        });
  }

  @Override
  @Nonnull
  public List<H> getEnabledHooks() {
    return hooks.stream()
        .filter(EventHook::isEnabled)
        .sorted(Comparator.comparing(EventHook::executionOrder))
        .toList();
  }

  @Override
  public void registerKafkaListener(
      @Nonnull KafkaListenerEndpoint kafkaListenerEndpoint, boolean startImmediately) {
    kafkaListenerEndpointRegistry.registerListenerContainer(
        kafkaListenerEndpoint, kafkaListenerContainerFactory, startImmediately);
  }

  @Override
  @Nonnull
  public KafkaListenerEndpoint createListenerEndpoint(
      @Nonnull String consumerGroupId, @Nonnull List<String> topics, @Nonnull List<H> groupHooks) {
    MethodKafkaListenerEndpoint<String, Object> kafkaListenerEndpoint =
        new MethodKafkaListenerEndpoint<>();
    kafkaListenerEndpoint.setId(consumerGroupId);
    kafkaListenerEndpoint.setGroupId(consumerGroupId);
    kafkaListenerEndpoint.setAutoStartup(false);
    kafkaListenerEndpoint.setTopics(topics.toArray(new String[0]));
    kafkaListenerEndpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());

    Map<String, Set<String>> aspectsToDrop = parseAspectsToDrop();

    GenericKafkaListener<E, H> listener =
        createListener(consumerGroupId, groupHooks, isFineGrainedLoggingEnabled(), aspectsToDrop);

    kafkaListenerEndpoint.setBean(listener);

    try {
      Method consumeMethod = GenericKafkaListener.class.getMethod("consume", ConsumerRecord.class);
      kafkaListenerEndpoint.setMethod(consumeMethod);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }

    return kafkaListenerEndpoint;
  }

  @Override
  @Nonnull
  public String buildConsumerGroupName(@Nonnull String suffix) {
    if (suffix.isEmpty()) {
      return consumerGroupBase;
    } else {
      return String.join("-", consumerGroupBase, suffix);
    }
  }

  /**
   * Gets the processor type name for logging.
   *
   * @return The processor type name
   */
  protected abstract String getProcessorType();

  /**
   * Gets the list of topic names to listen to.
   *
   * @return List of topic names
   */
  protected abstract List<String> getTopicNames();

  /**
   * Checks if fine-grained logging is enabled.
   *
   * @return true if fine-grained logging is enabled, false otherwise
   */
  protected abstract boolean isFineGrainedLoggingEnabled();

  /**
   * Parses the aspects to drop from configuration.
   *
   * @return Map of entity types to sets of aspect names to drop
   */
  protected Map<String, Set<String>> parseAspectsToDrop() {
    String aspectsToDropConfig = getAspectsToDropConfig();
    if (StringUtils.isBlank(aspectsToDropConfig)) {
      return Collections.emptyMap();
    } else {
      JavaType type =
          objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Set.class);
      try {
        return objectMapper.readValue(aspectsToDropConfig, type);
      } catch (Exception e) {
        log.error("Unable to parse aspects to drop configuration: {}", aspectsToDropConfig, e);
        return Collections.emptyMap();
      }
    }
  }

  /**
   * Gets the configuration string for aspects to drop.
   *
   * @return Configuration string
   */
  protected abstract String getAspectsToDropConfig();
}
