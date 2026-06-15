package com.linkedin.metadata.kafka;

import com.linkedin.metadata.kafka.config.DataHubUsageEventsProcessorCondition;
import com.linkedin.metadata.kafka.config.UsageEventsIndexingConfiguration;
import com.linkedin.metadata.kafka.transformer.DataHubUsageEventTransformer;
import com.linkedin.metadata.kafka.usage.DataHubUsageEventIndexer;
import com.linkedin.metadata.pgqueue.PgQueueStringDecode;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Conditional(DataHubUsageEventsProcessorCondition.class)
@Import({UsageEventsIndexingConfiguration.class})
public class DataHubUsageEventsProcessor {
  public static final String DATAHUB_USAGE_EVENT_KAFKA_CONSUMER_GROUP_VALUE =
      "${DATAHUB_USAGE_EVENT_KAFKA_CONSUMER_GROUP_ID:datahub-usage-event-consumer-job-client}";

  private final DataHubUsageEventIndexer usageEventIndexer;
  private final DataHubUsageEventTransformer dataHubUsageEventTransformer;
  private final OperationContext systemOperationContext;

  @Value(DATAHUB_USAGE_EVENT_KAFKA_CONSUMER_GROUP_VALUE)
  private String datahubUsageEventConsumerGroupId;

  public DataHubUsageEventsProcessor(
      DataHubUsageEventIndexer usageEventIndexer,
      DataHubUsageEventTransformer dataHubUsageEventTransformer,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
    this.usageEventIndexer = usageEventIndexer;
    this.dataHubUsageEventTransformer = dataHubUsageEventTransformer;
    this.systemOperationContext = systemOperationContext;
  }

  /**
   * Legacy entry — used by callers that haven't been sliced upstream. Equivalent to {@link
   * #consume(OperationContext, List)} with the system context.
   *
   * <p>The affinity-aware Kafka listener ({@link DataHubUsageEventsKafkaListener}) splits the
   * inbound batch into affinity slices and calls {@link #consume(OperationContext, List)} once per
   * slice with that slice's representative context.
   */
  public void consume(final List<ConsumerRecord<String, String>> consumerRecords) {
    consume(systemOperationContext, consumerRecords);
  }

  /**
   * Batch listener: Spring Kafka delivers a List of records per poll; we transform each, then hand
   * the whole batch to the indexer in a single {@link DataHubUsageEventIndexer#indexBatch(List)}
   * call so storage-aware impls (Postgres) can commit them in one transaction. Spans + lag metrics
   * are attributed under {@code sliceContext}, which is the slice's representative {@link
   * OperationContext} (the system context in OSS; a per-affinity context in deployments that supply
   * a custom {@code InboundBatchAffinityResolver}).
   */
  public void consume(
      @Nonnull final OperationContext sliceContext,
      final List<ConsumerRecord<String, String>> consumerRecords) {
    indexBatch(
        sliceContext,
        consumerRecords.stream()
            .map(
                record ->
                    new UsageEventInput(
                        record.topic(),
                        record.key(),
                        record.partition(),
                        record.offset(),
                        record.serializedValueSize(),
                        record.timestamp(),
                        record.value(),
                        () -> recordKafkaLag(record)))
            .toList());
  }

  /** pgQueue transport: same indexing path as {@link #consume(List)}. */
  public void consume(String logicalTopic, final List<QueueReceivedMessage> messages) {
    indexBatch(
        systemOperationContext,
        messages.stream()
            .map(
                msg -> {
                  String payload = PgQueueStringDecode.decodeAsUtf8(msg);
                  return new UsageEventInput(
                      logicalTopic,
                      msg.routingKey(),
                      msg.handle().partitionId(),
                      msg.handle().enqueueSeq(),
                      payload.length(),
                      msg.handle().enqueuedAt().toEpochMilli(),
                      payload,
                      () -> recordPgQueueLag(logicalTopic, msg));
                })
            .toList());
  }

  private void indexBatch(@Nonnull OperationContext sliceContext, List<UsageEventInput> inputs) {
    sliceContext.withSpan(
        "consume",
        () -> {
          List<DataHubUsageEventIndexer.IndexableUsageEvent> events =
              new ArrayList<>(inputs.size());
          for (UsageEventInput input : inputs) {
            input.recordLag().run();
            log.debug(
                "Got DUE event key: {}, topic: {}, partition: {}, offset: {}, value size: {}, timestamp: {}",
                input.key(),
                input.logicalTopic(),
                input.partition(),
                input.offset(),
                input.valueSize(),
                input.timestampMillis());

            Optional<DataHubUsageEventTransformer.TransformedDocument> eventDocument =
                dataHubUsageEventTransformer.transformDataHubUsageEvent(input.payload());
            if (eventDocument.isEmpty()) {
              log.warn("Failed to apply usage events transform to record: {}", input.payload());
              continue;
            }
            String documentId = generateDocumentId(eventDocument.get().getId(), input.offset());
            events.add(
                new DataHubUsageEventIndexer.IndexableUsageEvent(eventDocument.get(), documentId));
          }
          usageEventIndexer.indexBatch(events);
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "consume"));
  }

  private record UsageEventInput(
      String logicalTopic,
      String key,
      int partition,
      long offset,
      int valueSize,
      long timestampMillis,
      String payload,
      Runnable recordLag) {}

  private void recordPgQueueLag(String logicalTopic, QueueReceivedMessage msg) {
    systemOperationContext
        .getMetricUtils()
        .ifPresent(
            metricUtils ->
                MetricUtils.recordInboundMessageQueueLag(
                    metricUtils,
                    this.getClass(),
                    logicalTopic,
                    datahubUsageEventConsumerGroupId,
                    msg.handle().enqueuedAt().toEpochMilli(),
                    MetricUtils.MESSAGING_SYSTEM_PGQUEUE,
                    msg.priority()));
  }

  private void recordKafkaLag(ConsumerRecord<String, String> consumerRecord) {
    systemOperationContext
        .getMetricUtils()
        .ifPresent(
            metricUtils ->
                MetricUtils.recordInboundMessageQueueLag(
                    metricUtils,
                    this.getClass(),
                    consumerRecord.topic(),
                    datahubUsageEventConsumerGroupId,
                    consumerRecord.timestamp(),
                    MetricUtils.MESSAGING_SYSTEM_KAFKA,
                    null));
  }

  /**
   * DataHub Usage Event is written to an append-only index called a data stream. Due to
   * circumstances it is possible that the event's id, even though it contains an epoch millisecond,
   * results in duplicate ids in the index. The collisions will stall processing of the topic. To
   * prevent the collisions we append the last 5 digits, padded with zeros, of the kafka offset to
   * prevent the collision.
   *
   * @param eventId the event's id
   * @param kafkaOffset the kafka offset for the message
   * @return unique identifier for event
   */
  private static String generateDocumentId(String eventId, long kafkaOffset) {
    return URLEncoder.encode(
        String.format("%s_%05d", eventId, leastSignificant(kafkaOffset, 5)),
        StandardCharsets.UTF_8);
  }

  private static int leastSignificant(long kafkaOffset, int digits) {
    final String input = String.valueOf(kafkaOffset);
    if (input.length() > digits) {
      return Integer.parseInt(input.substring(input.length() - digits));
    } else {
      return Integer.parseInt(input);
    }
  }
}
