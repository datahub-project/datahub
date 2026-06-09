package com.linkedin.metadata.kafka.config;

import static com.linkedin.mxe.ConsumerGroups.MCP_CONSUMER_GROUP_ID_VALUE;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.MceConsumerConfiguration;
import com.linkedin.metadata.config.pgqueue.PgQueueConsumerPollSettings;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.kafka.InboundMetadataEnvelope;
import com.linkedin.metadata.kafka.InboundRecordProperties;
import com.linkedin.metadata.kafka.MetadataChangeProposalConsumer;
import com.linkedin.metadata.kafka.batch.BatchMetadataChangeProposalsProcessor;
import com.linkedin.metadata.pgqueue.PgQueuePollerRegistration;
import com.linkedin.metadata.pgqueue.PgQueuePollerSource;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.mxe.Topics;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * pgQueue worker registrations for MCE consumer — each bean is one parallel supervisor
 * (Kafka-style).
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class PgQueueMcePollerSourcesConfiguration {

  private final ConfigurationProvider configurationProvider;
  private final PostgresSqlSetupProperties postgresSqlSetupProperties;

  @Bean
  @Conditional(PgQueueMessagingAndMetadataChangeProposalProcessorCondition.class)
  public PgQueuePollerSource pgQueueMcpSource(
      MetadataChangeProposalConsumer consumer,
      @Value(MCP_CONSUMER_GROUP_ID_VALUE) String groupId,
      @Value("${METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.METADATA_CHANGE_PROPOSAL + "}")
          String topic) {
    PgQueueConsumerPollSettings.SleepMillis sleep = pollSleep();
    return () ->
        Stream.of(
            new PgQueuePollerRegistration(
                groupId,
                List.of(topic),
                metadataChangeProposalPollMaxBatch(),
                "pgqueue-" + groupId,
                sleep.emptyPoll(),
                sleep.missingTopic(),
                sleep.errorRecovery(),
                (logicalTopic, batch, ctx) -> {
                  for (QueueReceivedMessage msg : batch) {
                    try {
                      GenericRecord record = ctx.decodeAvro(msg, logicalTopic);
                      InboundMetadataEnvelope<GenericRecord> envelope =
                          InboundMetadataEnvelope.fromPgQueue(msg, logicalTopic, groupId, record);
                      consumer.accept(envelope, groupId);
                      ctx.commit(List.of(msg.handle()));
                    } catch (Exception e) {
                      log.error("MCP pgQueue message failed; lease will expire for retry", e);
                    }
                  }
                }));
  }

  @Bean
  @Conditional(PgQueueMessagingAndBatchMetadataChangeProposalProcessorCondition.class)
  public PgQueuePollerSource pgQueueBatchMcpSource(
      BatchMetadataChangeProposalsProcessor batchProcessor,
      @Value(MCP_CONSUMER_GROUP_ID_VALUE) String groupId,
      @Value("${METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.METADATA_CHANGE_PROPOSAL + "}")
          String topic) {
    PgQueueConsumerPollSettings.SleepMillis sleep = pollSleep();
    return () ->
        Stream.of(
            new PgQueuePollerRegistration(
                groupId,
                List.of(topic),
                batchMetadataChangeProposalPollMaxBatch(),
                "pgqueue-batch-mcp-" + groupId,
                sleep.emptyPoll(),
                sleep.missingTopic(),
                sleep.errorRecovery(),
                (logicalTopic, rawBatch, ctx) -> {
                  List<ConsumerRecord<String, GenericRecord>> synthetic =
                      new ArrayList<>(rawBatch.size());
                  List<InboundRecordProperties> inboundProperties =
                      new ArrayList<>(rawBatch.size());
                  List<QueueMessageHandle> handles = new ArrayList<>(rawBatch.size());
                  for (QueueReceivedMessage msg : rawBatch) {
                    GenericRecord record = ctx.decodeAvro(msg, logicalTopic);
                    var h = msg.handle();
                    synthetic.add(
                        new ConsumerRecord<>(
                            logicalTopic,
                            h.partitionId(),
                            h.enqueueSeq(),
                            msg.routingKey(),
                            record));
                    inboundProperties.add(
                        InboundRecordProperties.fromPgQueue(
                            h.enqueuedAt().toEpochMilli(), msg.priority()));
                    handles.add(h);
                  }
                  try {
                    batchProcessor.consume(synthetic, inboundProperties);
                    ctx.commit(handles);
                  } catch (Exception e) {
                    log.error("Batch MCP pgQueue processing failed; leases expire for retry", e);
                  }
                }));
  }

  int metadataChangeProposalPollMaxBatch() {
    return PgQueueConsumerPollSettings.requirePollMaxBatch(
        Optional.ofNullable(configurationProvider.getMceConsumer())
            .map(MceConsumerConfiguration::getPgQueue)
            .map(MceConsumerConfiguration.PgQueuePoll::getMetadataChangeProposalMaxBatch)
            .orElse(null),
        "mceConsumer.pgQueue.metadataChangeProposalMaxBatch");
  }

  int batchMetadataChangeProposalPollMaxBatch() {
    return PgQueueConsumerPollSettings.requirePollMaxBatch(
        Optional.ofNullable(configurationProvider.getMceConsumer())
            .map(MceConsumerConfiguration::getPgQueue)
            .map(MceConsumerConfiguration.PgQueuePoll::getBatchMetadataChangeProposalMaxBatch)
            .orElse(null),
        "mceConsumer.pgQueue.batchMetadataChangeProposalMaxBatch");
  }

  private PgQueueConsumerPollSettings.SleepMillis pollSleep() {
    return PgQueueConsumerPollSettings.requireSleep(postgresSqlSetupProperties);
  }
}
