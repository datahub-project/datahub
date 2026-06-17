package com.linkedin.metadata.kafka.batch;

import static com.linkedin.metadata.utils.metrics.MetricUtils.BATCH_SIZE_ATTR;
import static com.linkedin.mxe.ConsumerGroups.MCP_CONSUMER_GROUP_ID_VALUE;

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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
   * <p>The Kafka listener entry ({@link BatchMetadataChangeProposalsKafkaListener}) splits the
   * inbound batch into affinity slices and calls {@link #consume(OperationContext, List)} once per
   * slice with that slice's representative context.
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

    List<MetadataChangeProposal> allMCPs = new ArrayList<>(consumerRecords.size());
    String topicName = null;

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

      try {
        MetadataChangeProposal mcp = EventUtils.avroToPegasusMCP(record);
        allMCPs.add(mcp);
      } catch (IOException e) {
        log.error(
            "Unrecoverable message deserialization error. Cannot forward to failure topic.", e);
      }
    }

    // Create the span tracking for all records, even if allMCPs is empty
    List<SystemMetadata> systemMetadataList =
        allMCPs.stream().map(MetadataChangeProposal::getSystemMetadata).toList();

    sliceContext.withQueueSpan(
        "consume",
        systemMetadataList,
        topicName,
        () -> {
          if (!allMCPs.isEmpty()) {
            // Now partition and process within the span
            processInBatches(sliceContext, allMCPs);
          } else {
            log.info("No valid MCPs to process after deserialization");
          }
        },
        BATCH_SIZE_ATTR,
        String.valueOf(allMCPs.size()),
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "consume"));
  }

  /**
   * Process MCPs in batches within the established span
   *
   * @param sliceContext slice-level OperationContext used for downstream ingest + FMCP emission
   * @param allMCPs All MCPs to process
   */
  private void processInBatches(
      @Nonnull OperationContext sliceContext, List<MetadataChangeProposal> allMCPs) {
    List<MetadataChangeProposal> currentBatch = new ArrayList<>();
    long currentBatchSize = 0;
    int totalProcessed = 0;

    for (MetadataChangeProposal mcp : allMCPs) {
      long mcpSize = calculateMCPSize(mcp);

      // If adding this MCP would exceed the batch size limit, process the current batch first
      if (!currentBatch.isEmpty()
          && currentBatchSize + mcpSize
              > provider.getMetadataChangeProposal().getConsumer().getBatch().getSize()) {
        processBatch(sliceContext, currentBatch, currentBatchSize);
        totalProcessed += currentBatch.size();
        log.debug(
            "Processed batch of {} MCPs, total processed so far: {}/{}",
            currentBatch.size(),
            totalProcessed,
            allMCPs.size());

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
          allMCPs.size());
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
