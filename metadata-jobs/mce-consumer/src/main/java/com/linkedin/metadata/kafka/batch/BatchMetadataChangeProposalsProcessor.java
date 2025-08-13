package com.linkedin.metadata.kafka.batch;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.MCP_EVENT_CONSUMER_NAME;
import static com.linkedin.metadata.utils.metrics.MetricUtils.BATCH_SIZE_ATTR;
import static com.linkedin.mxe.ConsumerGroups.MCP_CONSUMER_GROUP_ID_VALUE;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entityclient.RestliEntityClientFactory;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.kafka.config.batch.BatchMetadataChangeProposalProcessorCondition;
import com.linkedin.metadata.kafka.util.KafkaListenerUtil;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Import({RestliEntityClientFactory.class})
@Conditional(BatchMetadataChangeProposalProcessorCondition.class)
@EnableKafka
@RequiredArgsConstructor
public class BatchMetadataChangeProposalsProcessor {
  private final OperationContext systemOperationContext;
  private final SystemEntityClient entityClient;
  private final EventProducer kafkaProducer;

  @Qualifier("kafkaThrottle")
  private final ThrottleSensor kafkaThrottle;

  private final KafkaListenerEndpointRegistry registry;
  private final ConfigurationProvider provider;

  @Value(
      "${FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME:"
          + Topics.FAILED_METADATA_CHANGE_PROPOSAL
          + "}")
  private String fmcpTopicName;

  @Value(MCP_CONSUMER_GROUP_ID_VALUE)
  private String mceConsumerGroupId;

  @PostConstruct
  public void registerConsumerThrottle() {
    KafkaListenerUtil.registerThrottle(kafkaThrottle, provider, registry, mceConsumerGroupId);
  }

  @KafkaListener(
      id = MCP_CONSUMER_GROUP_ID_VALUE,
      topics = "${METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.METADATA_CHANGE_PROPOSAL + "}",
      containerFactory = MCP_EVENT_CONSUMER_NAME,
      batch = "true",
      autoStartup = "false")
  public void consume(final List<ConsumerRecord<String, GenericRecord>> consumerRecords) {

    List<MetadataChangeProposal> allMCPs = new ArrayList<>(consumerRecords.size());
    String topicName = null;

    for (ConsumerRecord<String, GenericRecord> consumerRecord : consumerRecords) {
      systemOperationContext
          .getMetricUtils()
          .ifPresent(
              metricUtils -> {
                long queueTimeMs = System.currentTimeMillis() - consumerRecord.timestamp();

                // Dropwizard legacy
                metricUtils.histogram(this.getClass(), "kafkaLag", queueTimeMs);

                // Micrometer with tags
                // TODO: include priority level when available
                metricUtils
                    .getRegistry()
                    .ifPresent(
                        meterRegistry -> {
                          meterRegistry
                              .timer(
                                  MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
                                  "topic",
                                  consumerRecord.topic(),
                                  "consumer.group",
                                  mceConsumerGroupId)
                              .record(Duration.ofMillis(queueTimeMs));
                        });
              });
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

    systemOperationContext.withQueueSpan(
        "consume",
        systemMetadataList,
        topicName,
        () -> {
          if (!allMCPs.isEmpty()) {
            // Now partition and process within the span
            processInBatches(allMCPs);
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
   * @param allMCPs All MCPs to process
   */
  private void processInBatches(List<MetadataChangeProposal> allMCPs) {
    List<MetadataChangeProposal> currentBatch = new ArrayList<>();
    long currentBatchSize = 0;
    int totalProcessed = 0;

    for (MetadataChangeProposal mcp : allMCPs) {
      long mcpSize = calculateMCPSize(mcp);

      // If adding this MCP would exceed the batch size limit, process the current batch first
      if (!currentBatch.isEmpty()
          && currentBatchSize + mcpSize
              > provider.getMetadataChangeProposal().getConsumer().getBatch().getSize()) {
        processBatch(currentBatch, currentBatchSize);
        totalProcessed += currentBatch.size();
        log.info(
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
      processBatch(currentBatch, currentBatchSize);
      totalProcessed += currentBatch.size();
      log.info(
          "Processed final batch of {} MCPs, total processed: {}/{}",
          currentBatch.size(),
          totalProcessed,
          allMCPs.size());
    }
  }

  /**
   * Process a batch of MCPs
   *
   * @param batch The MCPs to process
   */
  private void processBatch(List<MetadataChangeProposal> batch, long batchBytes) {
    if (batch.isEmpty()) {
      return;
    }

    log.info(
        "Processing batch of {} records, total size approximately {} bytes",
        batch.size(),
        batchBytes);

    try {
      List<String> urns = entityClient.batchIngestProposals(systemOperationContext, batch, false);

      log.info(
          "Successfully processed MCP event batch of size {} with urns: {}",
          batch.size(),
          urns.stream().filter(Objects::nonNull).toList());
    } catch (Throwable throwable) {
      log.error("MCP Processor Error", throwable);
      Span currentSpan = Span.current();
      currentSpan.recordException(throwable);
      currentSpan.setStatus(StatusCode.ERROR, throwable.getMessage());
      currentSpan.setAttribute(MetricUtils.ERROR_TYPE, throwable.getClass().getName());

      kafkaProducer.produceFailedMetadataChangeProposal(systemOperationContext, batch, throwable);
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
