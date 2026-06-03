package com.linkedin.gms.factory.trace;

import static com.linkedin.mxe.ConsumerGroups.MCP_CONSUMER_GROUP_ID_VALUE;

import com.linkedin.metadata.config.messaging.PgQueueMessagingTransportCondition;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.trace.McpFailedTracePort;
import com.linkedin.metadata.trace.McpPendingTracePort;
import com.linkedin.metadata.trace.PgQueueMcpFailedTracePort;
import com.linkedin.metadata.trace.PgQueueMcpPendingTracePort;
import com.linkedin.mxe.Topics;
import java.util.concurrent.ExecutorService;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(PgQueueMessagingTransportCondition.class)
public class PgQueueTraceReadersFactory {

  @Value("${METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.METADATA_CHANGE_PROPOSAL + "}")
  private String mcpTopicName;

  @Value(
      "${FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME:"
          + Topics.FAILED_METADATA_CHANGE_PROPOSAL
          + "}")
  private String mcpFailedTopicName;

  @Value(MCP_CONSUMER_GROUP_ID_VALUE)
  private String mceConsumerGroupId;

  @Value("${trace.timeout-seconds:30}")
  private long traceTimeoutSeconds;

  @Value("${trace.pgqueue.peek-batch-limit:2000}")
  private int peekBatchLimit;

  @Value("${trace.pgqueue.peek-max-rounds:5}")
  private int peekMaxRounds;

  @Bean("mcpTraceReader")
  public McpPendingTracePort mcpTraceReader(
      MetadataQueueStore metadataQueueStore,
      @Qualifier("traceExecutorService") ExecutorService traceExecutorService,
      @Qualifier("pgQueueConsumerAvroDeserializer")
          Deserializer<GenericRecord> pgQueueConsumerAvroDeserializer) {
    return new PgQueueMcpPendingTracePort(
        metadataQueueStore,
        mcpTopicName,
        mceConsumerGroupId,
        traceExecutorService,
        traceTimeoutSeconds,
        peekBatchLimit,
        peekMaxRounds,
        pgQueueConsumerAvroDeserializer);
  }

  @Bean("mcpFailedTraceReader")
  public McpFailedTracePort mcpFailedTraceReader(
      MetadataQueueStore metadataQueueStore,
      @Qualifier("traceExecutorService") ExecutorService traceExecutorService,
      @Qualifier("pgQueueConsumerAvroDeserializer")
          Deserializer<GenericRecord> pgQueueConsumerAvroDeserializer) {
    return new PgQueueMcpFailedTracePort(
        metadataQueueStore,
        mcpFailedTopicName,
        traceExecutorService,
        traceTimeoutSeconds,
        peekBatchLimit,
        peekMaxRounds,
        pgQueueConsumerAvroDeserializer);
  }
}
