package com.linkedin.metadata.kafka;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.gms.factory.kafka.KafkaEventConsumerFactory;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.kafka.config.MetadataChangeLogProcessorCondition;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.kafka.hook.UpdateIndicesHook;
import com.linkedin.metadata.kafka.hook.event.EntityChangeEventGeneratorHook;
import com.linkedin.metadata.kafka.hook.ingestion.IngestionSchedulerHook;
import com.linkedin.metadata.kafka.hook.siblings.SiblingAssociationHook;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.Topics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Conditional(MetadataChangeLogProcessorCondition.class)
@Import({
    UpdateIndicesHook.class,
    IngestionSchedulerHook.class,
    EntityChangeEventGeneratorHook.class,
    KafkaEventConsumerFactory.class,
    SiblingAssociationHook.class
})
@EnableKafka
public class MetadataChangeLogProcessor {

  private final Histogram kafkaLagStats = MetricUtils.get().histogram(MetricRegistry.name(this.getClass(), "kafkaLag"));

  private static final String DEFAULT_GROUP = "DEFAULT_GROUP";
  private static final String GROUP_1 = "GROUP_1";
  private static final String GROUP_2 = "GROUP_2";
  private static final String GROUP_3 = "GROUP_3";
  private static final String GROUP_4 = "GROUP_4";

  private static final String HOOK_NAME_1 = "updateIndicesHook";
  private static final String HOOK_NAME_2 = "ingestionSchedulerHook";
  private static final String HOOK_NAME_3 = "entityChangeEventHook";
  private static final String HOOK_NAME_4 = "siblingAssociationHook";

  @Value("${kafka.mae.consumer.group.default:}")
  private String consumerGroupHooks;

  @Value("${kafka.mae.consumer.group.1:}")
  private String consumerGroup1Hooks;

  @Value("${kafka.mae.consumer.group.2:}")
  private String consumerGroup2Hooks;

  @Value("${kafka.mae.consumer.group.3:}")
  private String consumerGroup3Hooks;

  @Value("${kafka.mae.consumer.group.4:}")
  private String consumerGroup4Hooks;

  private final Map<String, List<MetadataChangeLogHook>> groupMap;

  @Autowired
  public MetadataChangeLogProcessor(
      @Nonnull final UpdateIndicesHook updateIndicesHook,
      @Nonnull final IngestionSchedulerHook ingestionSchedulerHook,
      @Nonnull final EntityChangeEventGeneratorHook entityChangeEventHook,
      @Nonnull final SiblingAssociationHook siblingAssociationHook
  ) {
    Map<Class, MetadataChangeLogHook> hookMap = new HashMap<>();
    hookMap.put(updateIndicesHook.getClass(), updateIndicesHook);
    hookMap.put(ingestionSchedulerHook.getClass(), ingestionSchedulerHook);
    hookMap.put(entityChangeEventHook.getClass(), entityChangeEventHook);
    hookMap.put(siblingAssociationHook.getClass(), siblingAssociationHook);
    hookMap.values().forEach(MetadataChangeLogHook::init);

    groupMap = new HashMap<>();
    groupMap.put(DEFAULT_GROUP, getHooksFromConfiguration(consumerGroupHooks, hookMap));
    groupMap.put(GROUP_1, getHooksFromConfiguration(consumerGroup1Hooks, hookMap));
    groupMap.put(GROUP_2, getHooksFromConfiguration(consumerGroup2Hooks, hookMap));
    groupMap.put(GROUP_3, getHooksFromConfiguration(consumerGroup3Hooks, hookMap));
    groupMap.put(GROUP_4, getHooksFromConfiguration(consumerGroup4Hooks, hookMap));
  }

  @KafkaListener(id = "${METADATA_CHANGE_LOG_KAFKA_CONSUMER_GROUP_ID:generic-mae-consumer-job-client}", topics = {
          "${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_VERSIONED + "}",
          "${METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_TIMESERIES
                  + "}"}, containerFactory = "kafkaEventConsumer")
  public void consumeForGroupDefault(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    try {
      consume(consumerRecord, DEFAULT_GROUP);
    } catch (Exception e) {
      log.error("Unexpected exception in the default MCL Kafka Listener. Exception: {}", e);
    }
  }

  @KafkaListener(id = "${METADATA_CHANGE_LOG_KAFKA_UPDATE_INDICES_CONSUMER_GROUP_ID:"
          + "generic-mae-consumer-job-client-update-indices-hook}", topics = {
          "${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_VERSIONED + "}",
          "${METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_TIMESERIES
                  + "}"}, containerFactory = "kafkaEventConsumerWithRetry")
  public void consumeForGroup1(final ConsumerRecord<String, GenericRecord> consumerRecord) throws Exception {
    consume(consumerRecord, GROUP_1);
  }

  @KafkaListener(id = "${METADATA_CHANGE_LOG_KAFKA_INGESTION_SCHEDULER_CONSUMER_GROUP_ID:"
          + "generic-mae-consumer-job-client-ingestion-scheduler-hook}", topics = {
          "${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_VERSIONED + "}",
          "${METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_TIMESERIES
                  + "}"}, containerFactory = "kafkaEventConsumerWithRetry")
  public void consumeForGroup2(final ConsumerRecord<String, GenericRecord> consumerRecord) throws Exception {
    consume(consumerRecord, GROUP_2);
  }

  @KafkaListener(id = "${METADATA_CHANGE_LOG_KAFKA_ENTITY_CHANGE_EVENT_GENERATOR_CONSUMER_GROUP_ID:"
          + "generic-mae-consumer-job-client-entity-change-event-generator-hook}", topics = {
          "${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_VERSIONED + "}",
          "${METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_TIMESERIES
                  + "}"}, containerFactory = "kafkaEventConsumerWithRetry")
  public void consumeForGroup3(final ConsumerRecord<String, GenericRecord> consumerRecord) throws Exception {
    consume(consumerRecord, GROUP_3);
  }

  @KafkaListener(id = "${METADATA_CHANGE_LOG_KAFKA_SIBLING_ASSOCIATION_CONSUMER_GROUP_ID:"
          + "generic-mae-consumer-job-client-sibling-association-hook}", topics = {
          "${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_VERSIONED + "}",
          "${METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_TIMESERIES
                  + "}"}, containerFactory = "kafkaEventConsumerWithRetry")
  public void consumeForGroup4(final ConsumerRecord<String, GenericRecord> consumerRecord) throws Exception {
    consume(consumerRecord, GROUP_4);
  }

  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord, String groupName) throws Exception {
    List<MetadataChangeLogHook> hooks = groupMap.get(groupName);
    if (hooks == null) {
      return;
    }
    for (MetadataChangeLogHook hook : hooks) {
      kafkaLagStats.update(System.currentTimeMillis() - consumerRecord.timestamp());
      final GenericRecord record = consumerRecord.value();
      log.debug("Got Generic MCL on topic: {}, partition: {}, offset: {}, hook: {}", consumerRecord.topic(), consumerRecord.partition(),
              consumerRecord.offset(), hook.getClass());
      MetricUtils.counter(this.getClass(), "received_mcl_count").inc();

      MetadataChangeLog event;
      try {
        event = EventUtils.avroToPegasusMCL(record);
        log.debug("Successfully converted Avro MCL to Pegasus MCL. urn: {}, key: {}", event.getEntityUrn(),
                event.getEntityKeyAspect());
      } catch (Exception e) {
        MetricUtils.counter(this.getClass(), "avro_to_pegasus_conversion_failure").inc();
        log.error("Error deserializing message due to: {}", e);
        log.error("Message: {}", record.toString());
        return;
      }

      log.debug("Invoking MCL hooks for urn: {}, key: {}", event.getEntityUrn(), event.getEntityKeyAspect());
      if (hook.isEnabled()) {
        try {
          hook.invoke(event);
          MetricUtils.counter(this.getClass(), groupName).inc();
          log.debug("Successfully completed MCL hook {} for urn: {}, key: {}", hook.getClass(), event.getEntityUrn(),
                  event.getEntityKeyAspect());
        } catch (Exception e) {
          MetricUtils.counter(this.getClass(), groupName + "_failure").inc();
          log.error("Failed to execute MCL hook with name: {}, error: {}", hook.getClass().getCanonicalName(), e);
          if (!groupName.equals(DEFAULT_GROUP)) {
            throw e;
          }
        }
      }
    }
  }

  private List<MetadataChangeLogHook> getHooksFromConfiguration(String propertyValue, Map<Class, MetadataChangeLogHook> hookMap) {
    if (StringUtils.isEmpty(propertyValue)) {
      return List.of();
    }
    List<MetadataChangeLogHook> hooks = new ArrayList<>();
    String[] hookNames = propertyValue.split(",");
    for (int i = 0; i < hookNames.length; i++) {
      String hookName = hookNames[i].trim();
      switch (hookName) {
        case HOOK_NAME_1 :
          addHookClassToList(hooks, UpdateIndicesHook.class, hookMap);
          break;
        case HOOK_NAME_2:
          addHookClassToList(hooks, IngestionSchedulerHook.class, hookMap);
          break;
        case HOOK_NAME_3:
          addHookClassToList(hooks, EntityChangeEventGeneratorHook.class, hookMap);
          break;
        case HOOK_NAME_4:
          addHookClassToList(hooks, SiblingAssociationHook.class, hookMap);
          break;
        default:
          log.error("Unsupported hook type " + hookName + ". Allowed values: "
                   + HOOK_NAME_1 + ", " + HOOK_NAME_2 + ", " + HOOK_NAME_3 + ", " + HOOK_NAME_4);
          break;
      }
    }
    return hooks;
  }

  private void addHookClassToList(List<MetadataChangeLogHook> hookList, Class hookClass, Map<Class, MetadataChangeLogHook> hookMap) {
    MetadataChangeLogHook hook = hookMap.remove(hookClass);
    if (hook != null) {
      hookList.add(hook);
    } else {
      log.warn(String.format("Hook %s is already added!", hookClass.getName()));
    }
  }

}
