package com.linkedin.metadata.kafka.listener.mcl;

import static com.linkedin.metadata.Constants.MDC_ASPECT_NAME;
import static com.linkedin.metadata.Constants.MDC_CHANGE_TYPE;
import static com.linkedin.metadata.Constants.MDC_ENTITY_TYPE;
import static com.linkedin.metadata.Constants.MDC_ENTITY_URN;

import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.kafka.context.inbound.InboundBatchAffinityResolver;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.kafka.listener.AbstractKafkaListener;
import com.linkedin.metadata.trace.TraceServiceImpl;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;

/**
 * Batch Kafka listener for MetadataChangeLog events that processes multiple events together for
 * better performance. This listener follows the same pattern as MCLKafkaListener but processes
 * events in batches.
 */
@Slf4j
public class MCLBatchKafkaListener
    extends AbstractKafkaListener<MetadataChangeLog, MetadataChangeLogHook, GenericRecord> {

  @Override
  @Nonnull
  public MetadataChangeLog convertRecord(@Nonnull GenericRecord record) throws IOException {
    return EventUtils.avroToPegasusMCL(record);
  }

  @Override
  protected void setMDCContext(MetadataChangeLog event) {
    Urn entityUrn = event.getEntityUrn();
    String aspectName = event.hasAspectName() ? event.getAspectName() : null;
    String entityType = event.hasEntityType() ? event.getEntityType() : null;
    ChangeType changeType = event.hasChangeType() ? event.getChangeType() : null;

    MDC.put(MDC_ENTITY_URN, Optional.ofNullable(entityUrn).map(Urn::toString).orElse(""));
    MDC.put(MDC_ASPECT_NAME, aspectName);
    MDC.put(MDC_ENTITY_TYPE, entityType);
    MDC.put(MDC_CHANGE_TYPE, Optional.ofNullable(changeType).map(ChangeType::toString).orElse(""));
  }

  @Override
  protected boolean shouldSkipProcessing(MetadataChangeLog event) {
    return MCLKafkaListener.shouldSkipMcl(event, aspectsToDrop);
  }

  @Override
  protected List<String> getFineGrainedLoggingAttributes(MetadataChangeLog event) {
    List<String> attributes = new ArrayList<>();

    if (!fineGrainedLoggingEnabled) {
      return attributes;
    }

    String aspectName = event.hasAspectName() ? event.getAspectName() : null;
    String entityType = event.hasEntityType() ? event.getEntityType() : null;
    ChangeType changeType = event.hasChangeType() ? event.getChangeType() : null;

    if (aspectName != null) {
      attributes.add(MetricUtils.ASPECT_NAME);
      attributes.add(aspectName);
    }

    if (entityType != null) {
      attributes.add(MetricUtils.ENTITY_TYPE);
      attributes.add(entityType);
    }

    if (changeType != null) {
      attributes.add(MetricUtils.CHANGE_TYPE);
      attributes.add(changeType.name());
    }

    return attributes;
  }

  @Override
  protected SystemMetadata getSystemMetadata(MetadataChangeLog event) {
    return event.getSystemMetadata();
  }

  @Override
  protected String getEventDisplayString(MetadataChangeLog event) {
    return String.format(
        "urn: %s, aspect name: %s, entity type: %s, change type: %s",
        event.getEntityUrn(),
        event.hasAspectName() ? event.getAspectName() : null,
        event.hasEntityType() ? event.getEntityType() : null,
        event.hasChangeType() ? event.getChangeType() : null);
  }

  @Override
  protected void updateMetrics(String hookName, MetadataChangeLog event) {
    systemOperationContext
        .getMetricUtils()
        .ifPresent(
            metricUtils -> {
              Long requestEpochMillis =
                  TraceServiceImpl.extractTraceIdEpochMillis(event.getSystemMetadata());
              if (requestEpochMillis != null) {
                long queueTimeMs = System.currentTimeMillis() - requestEpochMillis;

                // request
                metricUtils
                    .getRegistry()
                    .timer(MetricUtils.DATAHUB_REQUEST_HOOK_QUEUE_TIME, "hook", hookName)
                    .record(Duration.ofMillis(queueTimeMs));
              }
            });
  }

  @Override
  public void consumeBatch(
      @Nonnull final List<ConsumerRecord<String, GenericRecord>> consumerRecords) {
    if (consumerRecords.isEmpty()) {
      return;
    }

    // Per-record inbound queue-lag metric — process-level attribution, not per-slice.
    for (ConsumerRecord<String, GenericRecord> consumerRecord : consumerRecords) {
      systemOperationContext
          .getMetricUtils()
          .ifPresent(
              metricUtils ->
                  MetricUtils.recordInboundMessageQueueLag(
                      metricUtils,
                      this.getClass(),
                      consumerRecord.topic(),
                      consumerGroupId,
                      consumerRecord.timestamp(),
                      MetricUtils.MESSAGING_SYSTEM_KAFKA,
                      null));
    }
    final String topicName = consumerRecords.get(0).topic();

    // Partition the inbound batch into affinity slices. In OSS the default resolver returns a
    // single slice carrying the system context. Deployments that need to dispatch portions of the
    // batch independently register their own InboundBatchAffinityResolver bean (single-bean,
    // @ConditionalOnMissingBean shadows the OSS default).
    final List<InboundBatchAffinityResolver.Slice<GenericRecord>> slices =
        batchAffinityResolver.partition(consumerRecords, consumerGroupId, systemOperationContext);
    log.debug(
        "Partitioned batch of {} records into {} slice(s)", consumerRecords.size(), slices.size());

    for (InboundBatchAffinityResolver.Slice<GenericRecord> slice : slices) {

      // Decode + filter within the slice — failures are logged and dropped, same as the prior
      // single-call shape.
      List<MetadataChangeLog> mcls = new ArrayList<>(slice.records().size());
      for (ConsumerRecord<String, GenericRecord> consumerRecord : slice.records()) {
        try {
          MetadataChangeLog mcl = convertRecord(consumerRecord.value());
          if (!shouldSkipProcessing(mcl)) {
            mcls.add(mcl);
          } else {
            log.info("Skipping MCL event: {}", mcl);
          }
        } catch (IOException e) {
          log.error("Unrecoverable message deserialization error", e);
        }
      }

      if (mcls.isEmpty()) {
        log.info("No valid MCLs to process in slice after deserialization");
        continue;
      }

      processBatchWithHooks(slice.context(), mcls, topicName);
    }
  }

  /**
   * Process a single slice of MCLs with all registered hooks under the slice's representative
   * {@link OperationContext}. Callers must guarantee the {@code mcls} share an operational
   * identity; {@link #consumeBatch} does this via {@link #batchAffinityResolver}.
   */
  private void processBatchWithHooks(
      @Nonnull OperationContext sliceContext,
      @Nonnull List<MetadataChangeLog> mcls,
      String topicName) {
    sliceContext.withQueueSpan(
        "consume",
        mcls.stream().map(MetadataChangeLog::getSystemMetadata).collect(Collectors.toList()),
        topicName,
        () -> {
          log.info("Invoking hooks for batch of {} MCL events", mcls.size());

          // Process with each hook
          for (MetadataChangeLogHook hook : hooks) {
            final String hookName = hook.getClass().getSimpleName();

            sliceContext.withSpan(
                hookName,
                () -> {
                  log.debug("Invoking hook {} for batch of {} MCLs", hookName, mcls.size());
                  try {
                    // Always call invokeBatch - hooks that don't support batch processing
                    // will fall back to individual processing via the default implementation
                    hook.invokeBatch(sliceContext, mcls);

                    // Update metrics — attribute under the slice context so deployments with
                    // affinity-aware resolvers get per-slice counters; in the default case this
                    // is the system context and behavior matches today.
                    sliceContext
                        .getMetricUtils()
                        .ifPresent(
                            metricUtils -> {
                              metricUtils.increment(
                                  this.getClass(), hookName + "_batch_success", mcls.size());
                            });
                  } catch (Exception e) {
                    // Just skip this hook and continue - "at most once" processing
                    sliceContext
                        .getMetricUtils()
                        .ifPresent(
                            metricUtils ->
                                metricUtils.increment(
                                    this.getClass(), hookName + "_batch_failure", mcls.size()));
                    log.error(
                        "Failed to execute hook with name {} for batch of {} MCLs",
                        hook.getClass().getCanonicalName(),
                        mcls.size(),
                        e);

                    Span currentSpan = Span.current();
                    currentSpan.recordException(e);
                    currentSpan.setStatus(StatusCode.ERROR, e.getMessage());
                    currentSpan.setAttribute(MetricUtils.ERROR_TYPE, e.getClass().getName());
                  }
                },
                MetricUtils.DROPWIZARD_NAME,
                MetricUtils.name(this.getClass(), hookName + "_batch_latency"));
          }

          sliceContext
              .getMetricUtils()
              .ifPresent(
                  metricUtils ->
                      metricUtils.increment(
                          this.getClass(), consumerGroupId + "_consumed_event_count", mcls.size()));
          log.info("Successfully completed hooks for batch of {} MCL events", mcls.size());
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "consume"));
  }
}
