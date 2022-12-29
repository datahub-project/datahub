package com.linkedin.metadata.kafka;

import com.google.common.collect.ImmutableList;
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
import java.util.List;
import java.util.UUID;
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

  private final List<MetadataChangeLogHook> hooks;

  @Autowired
  public MetadataChangeLogProcessor(
      @Nonnull final UpdateIndicesHook updateIndicesHook,
      @Nonnull final IngestionSchedulerHook ingestionSchedulerHook,
      @Nonnull final EntityChangeEventGeneratorHook entityChangeEventHook,
      @Nonnull final SiblingAssociationHook siblingAssociationHook
  ) {
    this.hooks = ImmutableList.of(updateIndicesHook, ingestionSchedulerHook, entityChangeEventHook, siblingAssociationHook);
    this.hooks.forEach(MetadataChangeLogHook::init);
  }

  @KafkaListener(id = "${METADATA_CHANGE_LOG_KAFKA_CONSUMER_GROUP_ID:generic-mae-consumer-job-client}", topics = {
      "${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_VERSIONED + "}",
      "${METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_TIMESERIES
          + "}"}, containerFactory = "kafkaEventConsumer")
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    MetricUtils.updateHistogram(System.currentTimeMillis() - consumerRecord.timestamp(), this.getClass().getName(), "kafkaLag");
    final GenericRecord record = consumerRecord.value();
    log.debug("Got Generic MCL on topic: {}, partition: {}, offset: {}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
    MetricUtils.counterInc(this.getClass().getName(), "received_mcl_count");

    MetadataChangeLog event;
    try {
      event = EventUtils.avroToPegasusMCL(record);
      log.debug("Successfully converted Avro MCL to Pegasus MCL. urn: {}, key: {}", event.getEntityUrn(),
          event.getEntityKeyAspect());
    } catch (Exception e) {
      MetricUtils.counterInc(this.getClass().getName(), "avro_to_pegasus_conversion_failure");
      log.error("Error deserializing message due to: ", e);
      log.error("Message: {}", record.toString());
      return;
    }

    log.debug("Invoking MCL hooks for urn: {}, key: {}", event.getEntityUrn(), event.getEntityKeyAspect());

    // Here - plug in additional "custom processor hooks"
    for (MetadataChangeLogHook hook : this.hooks) {
      if (!hook.isEnabled()) {
        continue;
      }
      UUID ignored = MetricUtils.timerStart(this.getClass().getName(), hook.getClass().getSimpleName() + "_latency");
      try {
        hook.invoke(event);
      } catch (Exception e) {
        // Just skip this hook and continue. - Note that this represents "at most once" processing.
        MetricUtils.counterInc(this.getClass().getName(), hook.getClass().getSimpleName() + "_failure");
        log.error("Failed to execute MCL hook with name {}", hook.getClass().getCanonicalName(), e);
      } finally {
        MetricUtils.timerStop(ignored, this.getClass().getName(), hook.getClass().getSimpleName() + "_latency");
      }
    }
    // TODO: Manually commit kafka offsets after full processing.
    MetricUtils.counterInc(this.getClass().getName(), "consumed_mcl_count");
    log.debug("Successfully completed MCL hooks for urn: {}, key: {}", event.getEntityUrn(),
        event.getEntityKeyAspect());
  }
}
