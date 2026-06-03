package com.linkedin.metadata.kafka;

import static com.linkedin.metadata.Constants.MDC_ASPECT_NAME;
import static com.linkedin.metadata.Constants.MDC_CHANGE_TYPE;
import static com.linkedin.metadata.Constants.MDC_ENTITY_TYPE;
import static com.linkedin.metadata.Constants.MDC_ENTITY_URN;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.kafka.config.MetadataChangeProposalProcessorCondition;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.MDC;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Conditional(MetadataChangeProposalProcessorCondition.class)
@RequiredArgsConstructor
public class MetadataChangeProposalConsumer {

  private final OperationContext systemOperationContext;
  private final SystemEntityClient entityClient;
  private final EventProducer eventProducer;

  public void accept(InboundMetadataEnvelope<GenericRecord> envelope, String mceConsumerGroupId) {
    try {
      systemOperationContext
          .getMetricUtils()
          .ifPresent(
              metricUtils ->
                  MetricUtils.recordInboundMessageQueueLag(
                      metricUtils,
                      this.getClass(),
                      envelope.getLogicalTopic(),
                      mceConsumerGroupId,
                      envelope.getEnqueuedAtMillis(),
                      envelope.getMessagingSystem(),
                      envelope.getPriority()));

      final GenericRecord record = envelope.getPayload();

      log.info(
          "Got MCP event key: {}, topic: {}, partition: {}, offset: {}, value size: {}, timestamp: {}",
          envelope.getKey(),
          envelope.getLogicalTopic(),
          envelope.getKafkaPartition(),
          envelope.getKafkaOffset(),
          envelope.getSerializedValueSize(),
          envelope.getEnqueuedAtMillis());

      if (log.isDebugEnabled()) {
        log.debug("Record {}", record);
      }

      final MetadataChangeProposal event;
      try {
        event = EventUtils.avroToPegasusMCP(record);

        systemOperationContext.withQueueSpan(
            "consume",
            event.getSystemMetadata(),
            envelope.getLogicalTopic(),
            () -> {
              try {
                Urn entityUrn = event.getEntityUrn();
                String aspectName = event.hasAspectName() ? event.getAspectName() : null;
                String entityType = event.hasEntityType() ? event.getEntityType() : null;
                ChangeType changeType = event.hasChangeType() ? event.getChangeType() : null;
                MDC.put(
                    MDC_ENTITY_URN, Optional.ofNullable(entityUrn).map(Urn::toString).orElse(""));
                MDC.put(MDC_ASPECT_NAME, aspectName);
                MDC.put(MDC_ENTITY_TYPE, entityType);
                MDC.put(
                    MDC_CHANGE_TYPE,
                    Optional.ofNullable(changeType).map(ChangeType::toString).orElse(""));

                if (log.isDebugEnabled()) {
                  log.debug("MetadataChangeProposal {}", event);
                }
                entityClient.ingestProposal(systemOperationContext, event, false);

                log.info("Successfully processed MCP event urn: {}", event.getEntityUrn());
              } catch (Throwable throwable) {
                log.error("MCP Processor Error", throwable);
                log.error("Message: {}", record);
                Span currentSpan = Span.current();
                currentSpan.recordException(throwable);
                currentSpan.setStatus(StatusCode.ERROR, throwable.getMessage());
                currentSpan.setAttribute(MetricUtils.ERROR_TYPE, throwable.getClass().getName());

                eventProducer.produceFailedMetadataChangeProposal(
                    systemOperationContext, List.of(event), throwable);
              }
            },
            MetricUtils.DROPWIZARD_NAME,
            MetricUtils.name(this.getClass(), "consume"));
      } catch (IOException e) {
        log.error(
            "Unrecoverable message deserialization error. Cannot forward to failure topic.", e);
      }
    } finally {
      MDC.clear();
    }
  }
}
