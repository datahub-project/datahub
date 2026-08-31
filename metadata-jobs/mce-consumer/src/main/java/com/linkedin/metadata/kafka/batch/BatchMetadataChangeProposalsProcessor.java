package com.linkedin.metadata.kafka.batch;

import static com.linkedin.metadata.utils.metrics.MetricUtils.BATCH_SIZE_ATTR;
import static com.linkedin.mxe.ConsumerGroups.MCP_CONSUMER_GROUP_ID_VALUE;

import com.linkedin.data.template.StringMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entityclient.RestliEntityClientFactory;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.kafka.InboundRecordProperties;
import com.linkedin.metadata.kafka.config.batch.BatchMetadataChangeProposalProcessorCondition;
import com.linkedin.metadata.kafka.pause.ConsumerPauseSupport;
import com.linkedin.metadata.kafka.util.KafkaListenerUtil;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Import({RestliEntityClientFactory.class})
@Conditional(BatchMetadataChangeProposalProcessorCondition.class)
@RequiredArgsConstructor
public class BatchMetadataChangeProposalsProcessor {

  private static final String AVRO_SYSTEM_METADATA_FIELD = "systemMetadata";
  private static final String AVRO_PROPERTIES_FIELD = "properties";

  private static final Set<String> TELEMETRY_PROPERTY_KEYS =
      Set.of(
          SystemTelemetryContext.TELEMETRY_TRACE_KEY,
          SystemTelemetryContext.TELEMETRY_QUEUE_SPAN_KEY,
          SystemTelemetryContext.TELEMETRY_LOG_KEY,
          SystemTelemetryContext.TELEMETRY_ENQUEUED_AT);

  private final OperationContext systemOperationContext;
  private final SystemEntityClient entityClient;
  private final EventProducer kafkaProducer;

  @Qualifier("kafkaThrottle")
  private final ThrottleSensor kafkaThrottle;

  private final ConfigurationProvider provider;
  private final ConsumerPauseSupport consumerPauseSupport;

  @Value(
      "${FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME:"
          + Topics.FAILED_METADATA_CHANGE_PROPOSAL
          + "}")
  private String fmcpTopicName;

  @Value(MCP_CONSUMER_GROUP_ID_VALUE)
  private String mceConsumerGroupId;

  @PostConstruct
  public void registerConsumerThrottle() {
    KafkaListenerUtil.registerThrottle(
        kafkaThrottle, provider, consumerPauseSupport, mceConsumerGroupId);
  }

  /**
   * Default-context entry point — used by tests and pgQueue batch poller paths that haven't been
   * sliced upstream. Equivalent to {@link #consume(OperationContext, List)} with the system
   * context.
   *
   * <p>The Kafka listener entry ({@link MCPBatchKafkaListenerRegistrar}) splits the inbound batch
   * into affinity slices and calls {@link #consume(OperationContext, List)} once per slice with
   * that slice's representative context.
   */
  public void consume(final List<ConsumerRecord<String, GenericRecord>> consumerRecords) {
    consume(systemOperationContext, consumerRecords);
  }

  /**
   * Entry point used by the affinity-aware Kafka listener. {@code sliceContext} is the
   * representative {@link OperationContext} for this slice — it threads through the consume span,
   * {@link SystemEntityClient#batchIngestProposals(OperationContext, java.util.List, boolean)}, and
   * any failure handling.
   */
  public void consume(
      @Nonnull final OperationContext sliceContext,
      final List<ConsumerRecord<String, GenericRecord>> consumerRecords) {
    consume(
        sliceContext,
        consumerRecords,
        consumerRecords.stream().map(InboundRecordProperties::fromKafka).toList());
  }

  /**
   * pgQueue entry point — same shape as the legacy Kafka-only call. Per-record properties (queue
   * lag) are supplied externally; the slice context defaults to the system context (pgQueue batches
   * are sliced upstream in the poller if needed).
   */
  public void consume(
      final List<ConsumerRecord<String, GenericRecord>> consumerRecords,
      @Nullable final List<InboundRecordProperties> inboundProperties) {
    consume(systemOperationContext, consumerRecords, inboundProperties);
  }

  /**
   * @param sliceContext slice-level OperationContext used for downstream calls (spans,
   *     batchIngestProposals, FMCP emission)
   * @param inboundProperties when non-null, same length as {@code consumerRecords}; per-record
   *     queue lag metadata for Kafka and pgQueue transports
   */
  public void consume(
      @Nonnull final OperationContext sliceContext,
      final List<ConsumerRecord<String, GenericRecord>> consumerRecords,
      @Nullable final List<InboundRecordProperties> inboundProperties) {

    String topicName = null;
    List<SystemMetadata> tracingSystemMetadataList = new ArrayList<>();

    for (int i = 0; i < consumerRecords.size(); i++) {
      ConsumerRecord<String, GenericRecord> consumerRecord = consumerRecords.get(i);
      InboundRecordProperties props =
          inboundProperties != null
              ? inboundProperties.get(i)
              : InboundRecordProperties.fromKafka(consumerRecord);
      systemOperationContext
          .getMetricUtils()
          .ifPresent(
              metricUtils ->
                  MetricUtils.recordInboundMessageQueueLag(
                      metricUtils,
                      this.getClass(),
                      consumerRecord.topic(),
                      mceConsumerGroupId,
                      props.getEnqueuedAtMillis(),
                      props.getMessagingSystem(),
                      props.getPriority()));
      final GenericRecord record = consumerRecord.value();

      if (topicName == null) {
        topicName = consumerRecord.topic();
      }

      log.info(
          "Got MCP event key: {}, topic: {}, partition: {}, offset: {}, value size: {}, timestamp: {}",
          consumerRecord.key(),
          consumerRecord.topic(),
          consumerRecord.partition(),
          consumerRecord.offset(),
          consumerRecord.serializedValueSize(),
          consumerRecord.timestamp());

      SystemMetadata traceMetadata = getTelemetryTraceMetadata(record);
      if (traceMetadata != null) {
        tracingSystemMetadataList.add(traceMetadata);
      }
    }

    final String spanTopicName = topicName;
    sliceContext.withQueueSpan(
        "consume",
        tracingSystemMetadataList,
        spanTopicName,
        () -> {
          if (consumerRecords.isEmpty()) {
            log.info("No MCPs to process after deserialization");
            return;
          }
          processInBatches(sliceContext, consumerRecords);
        },
        BATCH_SIZE_ATTR,
        String.valueOf(consumerRecords.size()),
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "consume"));
  }

  /**
   * Stream-convert and sub-batch the poll's MCPs within the established span.
   *
   * <p>Each avro record is converted to a Pegasus MCP lazily and accumulated into a byte-budgeted
   * sub-batch; when the budget is exceeded the sub-batch is ingested and released, so only one
   * sub-batch of Pegasus MCPs is resident at a time (rather than the full poll).
   *
   * @param sliceContext slice-level OperationContext used for downstream ingest + FMCP emission
   * @param consumerRecords the avro records for this consume call
   */
  private void processInBatches(
      @Nonnull OperationContext sliceContext,
      List<ConsumerRecord<String, GenericRecord>> consumerRecords) {
    List<MetadataChangeProposal> currentBatch = new ArrayList<>();
    long currentBatchSize = 0;
    int totalProcessed = 0;

    for (ConsumerRecord<String, GenericRecord> consumerRecord : consumerRecords) {
      final GenericRecord record = consumerRecord.value();
      final MetadataChangeProposal mcp;
      try {
        mcp = EventUtils.avroToPegasusMCP(record);
      } catch (IOException e) {
        log.error(
            "Unrecoverable message deserialization error. Cannot forward to failure topic.", e);
        continue;
      }
      long mcpSize = calculateMCPSize(mcp);

      // If adding this MCP would exceed the batch size limit, process the current batch first.
      // The limit is read only once currentBatch is non-empty, so a poll whose records all fail
      // deserialization never touches the config provider (matching the pre-streaming behaviour).
      if (!currentBatch.isEmpty()
          && currentBatchSize + mcpSize
              > provider.getMetadataChangeProposal().getConsumer().getBatch().getSize()) {
        processBatch(sliceContext, currentBatch, currentBatchSize);
        totalProcessed += currentBatch.size();
        log.debug(
            "Processed batch of {} MCPs, total processed so far: {}/{}",
            currentBatch.size(),
            totalProcessed,
            consumerRecords.size());

        // Reset for the next batch
        currentBatch = new ArrayList<>();
        currentBatchSize = 0;
      }

      // Add to the current batch
      currentBatch.add(mcp);
      currentBatchSize += mcpSize;
    }

    // Process any remaining records in the final batch
    if (!currentBatch.isEmpty()) {
      processBatch(sliceContext, currentBatch, currentBatchSize);
      totalProcessed += currentBatch.size();
      log.debug(
          "Processed final batch of {} MCPs, total processed: {}/{}",
          currentBatch.size(),
          totalProcessed,
          consumerRecords.size());
    }
  }

  /**
   * Process a batch of MCPs under the supplied slice context.
   *
   * @param sliceContext slice-level OperationContext used for ingest + FMCP emission
   * @param batch The MCPs to process
   */
  private void processBatch(
      @Nonnull OperationContext sliceContext, List<MetadataChangeProposal> batch, long batchBytes) {
    if (batch.isEmpty()) {
      return;
    }

    log.debug(
        "Processing batch of {} records, total size approximately {} bytes",
        batch.size(),
        batchBytes);

    try {
      List<String> urns = entityClient.batchIngestProposals(sliceContext, batch, false);

      log.debug(
          "Successfully processed MCP event batch of size {} with urns: {}",
          batch.size(),
          urns.stream().filter(Objects::nonNull).toList());
    } catch (Throwable throwable) {
      log.error("MCP Processor Error", throwable);
      Span currentSpan = Span.current();
      currentSpan.recordException(throwable);
      currentSpan.setStatus(StatusCode.ERROR, throwable.getMessage());
      currentSpan.setAttribute(MetricUtils.ERROR_TYPE, throwable.getClass().getName());

      kafkaProducer.produceFailedMetadataChangeProposal(sliceContext, batch, throwable);
    }
  }

  /**
   * Read the telemetry trace properties off an avro MCP's {@code systemMetadata.properties} map
   * into a lightweight, properties-only {@link SystemMetadata} — without converting the
   * (potentially large) aspect via {@link EventUtils#avroToPegasusMCP}. Returns {@code null} unless
   * the record carries both {@link SystemTelemetryContext#TELEMETRY_TRACE_KEY} and {@link
   * SystemTelemetryContext#TELEMETRY_QUEUE_SPAN_KEY}, mirroring the filter {@link
   * SystemTelemetryContext#withQueueSpan} applies.
   *
   * <p>{@code withQueueSpan}/{@code closeQueueSpan} only read {@code properties} (the trace id,
   * queue span id, log-tracing flag, and enqueued-at timestamp) off each entry, so this trimmed
   * SystemMetadata is all they need to link the consume span to the upstream trace and record queue
   * latency on the receive span.
   */
  @Nullable
  private SystemMetadata getTelemetryTraceMetadata(@Nullable GenericRecord record) {
    if (record == null
        || !(record.get(AVRO_SYSTEM_METADATA_FIELD) instanceof GenericRecord systemMetadataRecord)
        || !(systemMetadataRecord.get(AVRO_PROPERTIES_FIELD) instanceof Map<?, ?> avroProperties)) {
      return null;
    }

    // Avro deserializes map keys as Utf8, which never equals a String, so we can't look them up by
    // key — iterate and stringify, copying only the telemetry entries into a Pegasus StringMap.
    StringMap telemetryProps = new StringMap();
    for (Map.Entry<?, ?> entry : avroProperties.entrySet()) {
      String key = entry.getKey().toString();
      if (entry.getValue() != null && TELEMETRY_PROPERTY_KEYS.contains(key)) {
        telemetryProps.put(key, entry.getValue().toString());
      }
    }

    // Both ids are required for withQueueSpan to link the consume span to the upstream trace.
    if (!telemetryProps.containsKey(SystemTelemetryContext.TELEMETRY_TRACE_KEY)
        || !telemetryProps.containsKey(SystemTelemetryContext.TELEMETRY_QUEUE_SPAN_KEY)) {
      return null;
    }

    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setProperties(telemetryProps);
    return systemMetadata;
  }

  /**
   * Calculate the size of a MetadataChangeProposal based on its aspect value
   *
   * @param mcp The MetadataChangeProposal
   * @return The size in bytes
   */
  private long calculateMCPSize(@Nullable MetadataChangeProposal mcp) {
    if (mcp == null) {
      return 0;
    }

    long size = 0;

    // Add size of aspect value if present
    if (mcp.getAspect() != null && mcp.getAspect().getValue() != null) {
      size += mcp.getAspect().getValue().length();
    }

    // Add a base size for the MCP structure itself and other fields
    size += 1000;

    return size;
  }
}
