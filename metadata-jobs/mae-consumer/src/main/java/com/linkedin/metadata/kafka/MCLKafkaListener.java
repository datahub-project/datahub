package com.linkedin.metadata.kafka;

import static com.linkedin.metadata.Constants.MDC_ASPECT_NAME;
import static com.linkedin.metadata.Constants.MDC_CHANGE_TYPE;
import static com.linkedin.metadata.Constants.MDC_ENTITY_TYPE;
import static com.linkedin.metadata.Constants.MDC_ENTITY_URN;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;

@Slf4j
public class MCLKafkaListener {
  private static final Histogram kafkaLagStats =
      MetricUtils.get()
          .histogram(
              MetricRegistry.name(
                  "com.linkedin.metadata.kafka.MetadataChangeLogProcessor", "kafkaLag"));

  private final String consumerGroupId;
  private final List<MetadataChangeLogHook> hooks;
  private final OperationContext systemOperationContext;

  public MCLKafkaListener(
      OperationContext systemOperationContext,
      String consumerGroup,
      List<MetadataChangeLogHook> hooks) {
    this.systemOperationContext = systemOperationContext;
    this.consumerGroupId = consumerGroup;
    this.hooks = hooks;
    this.hooks.forEach(hook -> hook.init(systemOperationContext));

    log.info(
        "Enabled MCL Hooks - Group: {} Hooks: {}",
        consumerGroup,
        hooks.stream().map(hook -> hook.getClass().getSimpleName()).collect(Collectors.toList()));
  }

  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    try {
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

      Urn entityUrn = event.getEntityUrn();
      String aspectName = event.hasAspectName() ? event.getAspectName() : null;
      String entityType = event.hasEntityType() ? event.getEntityType() : null;
      ChangeType changeType = event.hasChangeType() ? event.getChangeType() : null;
      MDC.put(MDC_ENTITY_URN, Optional.ofNullable(entityUrn).map(Urn::toString).orElse(""));
      MDC.put(MDC_ASPECT_NAME, aspectName);
      MDC.put(MDC_ENTITY_TYPE, entityType);
      MDC.put(
          MDC_CHANGE_TYPE, Optional.ofNullable(changeType).map(ChangeType::toString).orElse(""));

      systemOperationContext.withQueueSpan(
          "consume",
          event.getSystemMetadata(),
          consumerRecord.topic(),
          () -> {
            log.info(
                "Invoking MCL hooks for consumer: {} urn: {}, aspect name: {}, entity type: {}, change type: {}",
                consumerGroupId,
                entityUrn,
                aspectName,
                entityType,
                changeType);

            // Here - plug in additional "custom processor hooks"
            for (MetadataChangeLogHook hook : this.hooks) {
              systemOperationContext.withSpan(
                  hook.getClass().getSimpleName(),
                  () -> {
                    log.debug(
                        "Invoking MCL hook {} for urn: {}",
                        hook.getClass().getSimpleName(),
                        event.getEntityUrn());
                    try {
                      hook.invoke(event);
                    } catch (Exception e) {
                      // Just skip this hook and continue. - Note that this represents "at most
                      // once"//
                      // processing.
                      MetricUtils.counter(
                              this.getClass(), hook.getClass().getSimpleName() + "_failure")
                          .inc();
                      log.error(
                          "Failed to execute MCL hook with name {}",
                          hook.getClass().getCanonicalName(),
                          e);

                      Span currentSpan = Span.current();
                      currentSpan.recordException(e);
                      currentSpan.setStatus(StatusCode.ERROR, e.getMessage());
                      currentSpan.setAttribute(MetricUtils.ERROR_TYPE, e.getClass().getName());
                    }
                  },
                  MetricUtils.DROPWIZARD_NAME,
                  MetricUtils.name(this.getClass(), hook.getClass().getSimpleName() + "_latency"));
            }

            // TODO: Manually commit kafka offsets after full processing.
            MetricUtils.counter(this.getClass(), consumerGroupId + "_consumed_mcl_count").inc();
            log.info(
                "Successfully completed MCL hooks for consumer: {} urn: {}",
                consumerGroupId,
                event.getEntityUrn());
          },
          MetricUtils.DROPWIZARD_NAME,
          MetricUtils.name(this.getClass(), "consume"));
    } finally {
      MDC.clear();
    }
  }
}
