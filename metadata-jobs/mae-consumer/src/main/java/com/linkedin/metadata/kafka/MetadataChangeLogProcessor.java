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
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
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

  private final Map<Class, MetadataChangeLogHook> hookMap;
  private final Histogram kafkaLagStats = MetricUtils.get().histogram(MetricRegistry.name(this.getClass(), "kafkaLag"));

  @Autowired
  public MetadataChangeLogProcessor(
      @Nonnull final UpdateIndicesHook updateIndicesHook,
      @Nonnull final IngestionSchedulerHook ingestionSchedulerHook,
      @Nonnull final EntityChangeEventGeneratorHook entityChangeEventHook,
      @Nonnull final SiblingAssociationHook siblingAssociationHook
  ) {
    hookMap = new HashMap<>();
    // Here - plug in additional "custom processor hooks" and create handler
    hookMap.put(updateIndicesHook.getClass(), updateIndicesHook);
    hookMap.put(ingestionSchedulerHook.getClass(), ingestionSchedulerHook);
    hookMap.put(entityChangeEventHook.getClass(), entityChangeEventHook);
    hookMap.put(siblingAssociationHook.getClass(), siblingAssociationHook);
    hookMap.values().forEach(MetadataChangeLogHook::init);
  }

  @KafkaListener(id = "${METADATA_CHANGE_LOG_KAFKA_UPDATE_INDICES_CONSUMER_GROUP_ID:"
          + "generic-mae-consumer-job-client-update-indices-hook}", topics = {
          "${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_VERSIONED + "}",
          "${METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_TIMESERIES
                  + "}"}, containerFactory = "kafkaEventConsumerWithRetry")
  public void consumeForUpdateIndicesHook(final ConsumerRecord<String, GenericRecord> consumerRecord) throws Exception {
    consume(consumerRecord, UpdateIndicesHook.class);
  }

  @KafkaListener(id = "${METADATA_CHANGE_LOG_KAFKA_INGESTION_SCHEDULER_CONSUMER_GROUP_ID:"
          + "generic-mae-consumer-job-client-ingestion-scheduler-hook}", topics = {
          "${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_VERSIONED + "}",
          "${METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_TIMESERIES
                  + "}"}, containerFactory = "kafkaEventConsumerWithRetry")
  public void consumeForIngestionSchedulerHook(final ConsumerRecord<String, GenericRecord> consumerRecord) throws Exception {
    consume(consumerRecord, IngestionSchedulerHook.class);
  }

  @KafkaListener(id = "${METADATA_CHANGE_LOG_KAFKA_ENTITY_CHANGE_EVENT_GENERATOR_CONSUMER_GROUP_ID:"
          + "generic-mae-consumer-job-client-entity-change-event-generator-hook}", topics = {
          "${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_VERSIONED + "}",
          "${METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_TIMESERIES
                  + "}"}, containerFactory = "kafkaEventConsumerWithRetry")
  public void consumeForEntityChangeEventGeneratorHook(final ConsumerRecord<String, GenericRecord> consumerRecord) throws Exception {
    consume(consumerRecord, EntityChangeEventGeneratorHook.class);
  }

  @KafkaListener(id = "${METADATA_CHANGE_LOG_KAFKA_SIBLING_ASSOCIATION_CONSUMER_GROUP_ID:"
          + "generic-mae-consumer-job-client-sibling-association-hook}", topics = {
          "${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_VERSIONED + "}",
          "${METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_TIMESERIES
                  + "}"}, containerFactory = "kafkaEventConsumerWithRetry")
  public void consumeForSiblingAssociationHook(final ConsumerRecord<String, GenericRecord> consumerRecord) throws Exception {
    consume(consumerRecord, SiblingAssociationHook.class);
  }

  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord, Class clazz) throws Exception {
    kafkaLagStats.update(System.currentTimeMillis() - consumerRecord.timestamp());
    MetadataChangeLogHook hook = hookMap.get(clazz);
    if (hook == null) {
      log.debug("Unable to retrieve hook for: {}", clazz.toString());
      return;
    }
    final GenericRecord record = consumerRecord.value();
    log.debug("Got Generic MCL on topic: {}, partition: {}, offset: {}, hook: {}", consumerRecord.topic(), consumerRecord.partition(),
            consumerRecord.offset(), clazz);
    MetricUtils.counter(this.getClass(), "received_mcl_count").inc();

    MetadataChangeLog event;
    try {
      event = EventUtils.avroToPegasusMCL(record);
      log.debug("Successfully converted Avro MCL to Pegasus MCL. urn: {}, key: {}", event.getEntityUrn(),
          event.getEntityKeyAspect());
    } catch (Exception e) {
      MetricUtils.counter(this.getClass(), "avro_to_pegasus_conversion_failure").inc();
      log.error("Error deserializing message due to: ", e);
      log.error("Message: {}", record.toString());
      return;
    }

    log.debug("Invoking MCL hooks for urn: {}, key: {}", event.getEntityUrn(), event.getEntityKeyAspect());
    if (hook.isEnabled()) {
      hook.invoke(event);
      MetricUtils.counter(this.getClass(), "consumed_mcl_count").inc();
      log.debug("Successfully completed MCL hook {} for urn: {}, key: {}", clazz, event.getEntityUrn(),
              event.getEntityKeyAspect());
    }
  }
}
