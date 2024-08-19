package com.linkedin.metadata.kafka;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Slf4j
public class MCLKafkaListener {
  private static final Histogram kafkaLagStats =
      MetricUtils.get()
          .histogram(
              MetricRegistry.name(
                  "com.linkedin.metadata.kafka.MetadataChangeLogProcessor", "kafkaLag"));

  private final String consumerGroupId;
  private final List<MetadataChangeLogHook> hooks;

  public MCLKafkaListener(
      OperationContext systemOperationContext,
      String consumerGroup,
      List<MetadataChangeLogHook> hooks) {
    this.consumerGroupId = consumerGroup;
    this.hooks = hooks;
    this.hooks.forEach(hook -> hook.init(systemOperationContext));

    log.info(
        "Enabled MCL Hooks - Group: {} Hooks: {}",
        consumerGroup,
        hooks.stream().map(hook -> hook.getClass().getSimpleName()).collect(Collectors.toList()));
  }

  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    try (Timer.Context i = MetricUtils.timer(this.getClass(), "consume").time()) {
      kafkaLagStats.update(System.currentTimeMillis() - consumerRecord.timestamp());
      final GenericRecord record = consumerRecord.value();
      log.debug(
          "Got MCL event consumer: {} key: {}, topic: {}, partition: {}, offset: {}, value size: {}, timestamp: {}",
          consumerGroupId,
          consumerRecord.key(),
          consumerRecord.topic(),
          consumerRecord.partition(),
          consumerRecord.offset(),
          consumerRecord.serializedValueSize(),
          consumerRecord.timestamp());
      MetricUtils.counter(this.getClass(), consumerGroupId + "_received_mcl_count").inc();

      MetadataChangeLog event;
      try {
        event = EventUtils.avroToPegasusMCL(record);
      } catch (Exception e) {
        MetricUtils.counter(
                this.getClass(), consumerGroupId + "_avro_to_pegasus_conversion_failure")
            .inc();
        log.error("Error deserializing message due to: ", e);
        log.error("Message: {}", record.toString());
        return;
      }

      log.info(
          "Invoking MCL hooks for consumer: {} urn: {}, aspect name: {}, entity type: {}, change type: {}",
          consumerGroupId,
          event.getEntityUrn(),
          event.hasAspectName() ? event.getAspectName() : null,
          event.hasEntityType() ? event.getEntityType() : null,
          event.hasChangeType() ? event.getChangeType() : null);

      // Here - plug in additional "custom processor hooks"
      for (MetadataChangeLogHook hook : this.hooks) {
        log.info(
            "Invoking MCL hook {} for urn: {}",
            hook.getClass().getSimpleName(),
            event.getEntityUrn());
        try (Timer.Context ignored =
            MetricUtils.timer(this.getClass(), hook.getClass().getSimpleName() + "_latency")
                .time()) {
          hook.invoke(event);
        } catch (Exception e) {
          // Just skip this hook and continue. - Note that this represents "at most once"//
          // processing.
          MetricUtils.counter(this.getClass(), hook.getClass().getSimpleName() + "_failure").inc();
          log.error(
              "Failed to execute MCL hook with name {}", hook.getClass().getCanonicalName(), e);
        }
      }
      // TODO: Manually commit kafka offsets after full processing.
      MetricUtils.counter(this.getClass(), consumerGroupId + "_consumed_mcl_count").inc();
      log.info(
          "Successfully completed MCL hooks for consumer: {} urn: {}",
          consumerGroupId,
          event.getEntityUrn());
    }
  }
}
