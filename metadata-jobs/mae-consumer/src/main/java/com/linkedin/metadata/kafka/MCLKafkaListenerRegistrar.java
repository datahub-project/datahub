package com.linkedin.metadata.kafka;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.MCL_EVENT_CONSUMER_NAME;

import com.linkedin.metadata.kafka.config.MetadataChangeLogProcessorCondition;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.stereotype.Component;

@Slf4j
@EnableKafka
@Component
@Conditional(MetadataChangeLogProcessorCondition.class)
public class MCLKafkaListenerRegistrar implements InitializingBean {

  @Autowired
  @Qualifier("systemOperationContext")
  private OperationContext systemOperationContext;

  @Autowired private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

  @Autowired
  @Qualifier(MCL_EVENT_CONSUMER_NAME)
  private KafkaListenerContainerFactory<?> kafkaListenerContainerFactory;

  @Value("${METADATA_CHANGE_LOG_KAFKA_CONSUMER_GROUP_ID:generic-mae-consumer-job-client}")
  private String consumerGroupBase;

  @Value("${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_VERSIONED + "}")
  private String mclVersionedTopicName;

  @Value(
      "${METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_TIMESERIES + "}")
  private String mclTimeseriesTopicName;

  @Autowired private List<MetadataChangeLogHook> metadataChangeLogHooks;

  @Override
  public void afterPropertiesSet() {
    Map<String, List<MetadataChangeLogHook>> hookGroups =
        getMetadataChangeLogHooks().stream()
            .collect(Collectors.groupingBy(MetadataChangeLogHook::getConsumerGroupSuffix));

    log.info(
        "MetadataChangeLogProcessor Consumer Groups: {}",
        hookGroups.keySet().stream().map(this::buildConsumerGroupName).collect(Collectors.toSet()));

    hookGroups.forEach(
        (key, hooks) -> {
          KafkaListenerEndpoint kafkaListenerEndpoint =
              createListenerEndpoint(
                  buildConsumerGroupName(key),
                  List.of(mclVersionedTopicName, mclTimeseriesTopicName),
                  hooks);
          registerMCLKafkaListener(kafkaListenerEndpoint, false);
        });
  }

  public List<MetadataChangeLogHook> getMetadataChangeLogHooks() {
    return metadataChangeLogHooks.stream()
        .filter(MetadataChangeLogHook::isEnabled)
        .sorted(Comparator.comparing(MetadataChangeLogHook::executionOrder))
        .toList();
  }

  @SneakyThrows
  public void registerMCLKafkaListener(
      KafkaListenerEndpoint kafkaListenerEndpoint, boolean startImmediately) {
    kafkaListenerEndpointRegistry.registerListenerContainer(
        kafkaListenerEndpoint, kafkaListenerContainerFactory, startImmediately);
  }

  private KafkaListenerEndpoint createListenerEndpoint(
      String consumerGroupId, List<String> topics, List<MetadataChangeLogHook> hooks) {
    MethodKafkaListenerEndpoint<String, GenericRecord> kafkaListenerEndpoint =
        new MethodKafkaListenerEndpoint<>();
    kafkaListenerEndpoint.setId(consumerGroupId);
    kafkaListenerEndpoint.setGroupId(consumerGroupId);
    kafkaListenerEndpoint.setAutoStartup(false);
    kafkaListenerEndpoint.setTopics(topics.toArray(new String[topics.size()]));
    kafkaListenerEndpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());
    kafkaListenerEndpoint.setBean(
        new MCLKafkaListener(systemOperationContext, consumerGroupId, hooks));
    try {
      kafkaListenerEndpoint.setMethod(
          MCLKafkaListener.class.getMethod("consume", ConsumerRecord.class));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }

    return kafkaListenerEndpoint;
  }

  private String buildConsumerGroupName(@Nonnull String suffix) {
    if (suffix.isEmpty()) {
      return consumerGroupBase;
    } else {
      return String.join("-", consumerGroupBase, suffix);
    }
  }
}
