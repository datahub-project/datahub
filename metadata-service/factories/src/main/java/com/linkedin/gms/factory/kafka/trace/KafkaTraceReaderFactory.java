package com.linkedin.gms.factory.kafka.trace;

import static com.linkedin.gms.factory.kafka.common.AdminClientFactory.buildKafkaAdminClient;
import static com.linkedin.mxe.ConsumerGroups.MCP_CONSUMER_GROUP_ID_VALUE;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.messaging.KafkaMessagingEnabled;
import com.linkedin.metadata.trace.MCLTraceReader;
import com.linkedin.metadata.trace.MCPFailedTraceReader;
import com.linkedin.metadata.trace.MCPTraceReader;
import com.linkedin.metadata.trace.TraceConsumerPool;
import com.linkedin.mxe.Topics;
import java.util.concurrent.ExecutorService;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// All beans here read Kafka admin/consumer state for message tracing. They cannot be created when
// the messaging transport is not Kafka (e.g. pgqueue), since AdminClient construction requires a
// valid bootstrap.servers and the topics don't exist on non-Kafka transports.
@Configuration
@KafkaMessagingEnabled
public class KafkaTraceReaderFactory {

  @Value("${trace.pollMaxAttempts:5}")
  private int pollMaxAttempts;

  @Value("${trace.pollDurationMs:1000}")
  private int pollDurationMs;

  @Value(MCP_CONSUMER_GROUP_ID_VALUE)
  private String mceConsumerGroupId;

  @Value("${METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.METADATA_CHANGE_PROPOSAL + "}")
  private String mcpTopicName;

  @Value(
      "${FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME:"
          + Topics.FAILED_METADATA_CHANGE_PROPOSAL
          + "}")
  private String mcpFailedTopicName;

  @Value("${METADATA_CHANGE_LOG_KAFKA_CONSUMER_GROUP_ID:generic-mae-consumer-job-client}")
  private String maeConsumerGroupId;

  @Value("${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_VERSIONED + "}")
  private String mclVersionedTopicName;

  @Value(
      "${METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_TIMESERIES + "}")
  private String mclTimeseriesTopicName;

  @Value("${trace.timeout-seconds:30}")
  private long traceTimeoutSeconds;

  @Value("${trace.futures.cancel-on-timeout:true}")
  private boolean cancelFuturesOnTimeout;

  @Bean("traceAdminClient")
  public AdminClient traceAdminClient(
      @Qualifier("configurationProvider") ConfigurationProvider provider,
      final KafkaProperties kafkaProperties) {
    return buildKafkaAdminClient(provider.getKafka(), kafkaProperties, "trace-reader");
  }

  @Bean("mcpTraceReader")
  public MCPTraceReader mcpTraceReader(
      @Qualifier("traceAdminClient") AdminClient adminClient,
      @Qualifier("mcpTraceConsumerPool") TraceConsumerPool mcpTraceConsumerPool,
      @Qualifier("traceExecutorService") ExecutorService traceExecutorService) {
    return MCPTraceReader.builder()
        .adminClient(adminClient)
        .topicName(mcpTopicName)
        .consumerGroupId(mceConsumerGroupId)
        .consumerPool(mcpTraceConsumerPool)
        .pollDurationMs(pollDurationMs)
        .pollMaxAttempts(pollMaxAttempts)
        .timeoutSeconds(traceTimeoutSeconds)
        .cancelFuturesOnTimeout(cancelFuturesOnTimeout)
        .executorService(traceExecutorService)
        .build();
  }

  @Bean("mcpFailedTraceReader")
  public MCPFailedTraceReader mcpFailedTraceReader(
      @Qualifier("traceAdminClient") AdminClient adminClient,
      @Qualifier("mcpFailedTraceConsumerPool") TraceConsumerPool mcpFailedTraceConsumerPool,
      @Qualifier("traceExecutorService") ExecutorService traceExecutorService) {
    return MCPFailedTraceReader.builder()
        .adminClient(adminClient)
        .topicName(mcpFailedTopicName)
        .consumerPool(mcpFailedTraceConsumerPool)
        .pollDurationMs(pollDurationMs)
        .pollMaxAttempts(pollMaxAttempts)
        .timeoutSeconds(traceTimeoutSeconds)
        .cancelFuturesOnTimeout(cancelFuturesOnTimeout)
        .executorService(traceExecutorService)
        .build();
  }

  @Bean("mclVersionedTraceReader")
  public MCLTraceReader mclVersionedTraceReader(
      @Qualifier("traceAdminClient") AdminClient adminClient,
      @Qualifier("mclVersionedTraceConsumerPool") TraceConsumerPool mclVersionedTraceConsumerPool,
      @Qualifier("traceExecutorService") ExecutorService traceExecutorService) {
    return MCLTraceReader.builder()
        .adminClient(adminClient)
        .topicName(mclVersionedTopicName)
        .consumerGroupId(maeConsumerGroupId)
        .consumerPool(mclVersionedTraceConsumerPool)
        .pollDurationMs(pollDurationMs)
        .pollMaxAttempts(pollMaxAttempts)
        .timeoutSeconds(traceTimeoutSeconds)
        .cancelFuturesOnTimeout(cancelFuturesOnTimeout)
        .executorService(traceExecutorService)
        .build();
  }

  @Bean("mclTimeseriesTraceReader")
  public MCLTraceReader mclTimeseriesTraceReader(
      @Qualifier("traceAdminClient") AdminClient adminClient,
      @Qualifier("mclTimeseriesTraceConsumerPool") TraceConsumerPool mclTimeseriesTraceConsumerPool,
      @Qualifier("traceExecutorService") ExecutorService traceExecutorService) {
    return MCLTraceReader.builder()
        .adminClient(adminClient)
        .topicName(mclTimeseriesTopicName)
        .consumerGroupId(maeConsumerGroupId)
        .consumerPool(mclTimeseriesTraceConsumerPool)
        .pollDurationMs(pollDurationMs)
        .pollMaxAttempts(pollMaxAttempts)
        .timeoutSeconds(traceTimeoutSeconds)
        .cancelFuturesOnTimeout(cancelFuturesOnTimeout)
        .executorService(traceExecutorService)
        .build();
  }
}
