package com.linkedin.gms.factory.kafka.trace;

import static com.linkedin.gms.factory.kafka.common.AdminClientFactory.buildKafkaAdminClient;
import static com.linkedin.mxe.ConsumerGroups.MCP_CONSUMER_GROUP_ID_VALUE;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.trace.MCLTraceReader;
import com.linkedin.metadata.trace.MCPFailedTraceReader;
import com.linkedin.metadata.trace.MCPTraceReader;
import com.linkedin.mxe.Topics;
import jakarta.annotation.Nonnull;
import jakarta.annotation.PreDestroy;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
public class KafkaTraceReaderFactory {
  private static final Properties TRACE_CONSUMER_PROPERTIES = new Properties();

  static {
    TRACE_CONSUMER_PROPERTIES.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
  }

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

  @Value("${trace.executor.thread-pool-size:10}")
  private int threadPoolSize;

  @Value("${trace.executor.shutdown-timeout-seconds:60}")
  private int shutdownTimeoutSeconds;

  @Value("${trace.timeout-seconds:30}")
  private long traceTimeoutSeconds;

  @Bean("traceAdminClient")
  public AdminClient traceAdminClient(
      @Qualifier("configurationProvider") ConfigurationProvider provider,
      final KafkaProperties kafkaProperties) {
    return buildKafkaAdminClient(provider.getKafka(), kafkaProperties, "trace-reader");
  }

  private ExecutorService traceExecutorService;

  @Bean("traceExecutorService")
  public ExecutorService traceExecutorService() {
    traceExecutorService = Executors.newFixedThreadPool(threadPoolSize);
    return traceExecutorService;
  }

  @Bean("mcpTraceReader")
  public MCPTraceReader mcpTraceReader(
      @Qualifier("traceAdminClient") AdminClient adminClient,
      @Qualifier("kafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      @Qualifier("traceExecutorService") ExecutorService traceExecutorService) {
    return MCPTraceReader.builder()
        .adminClient(adminClient)
        .topicName(mcpTopicName)
        .consumerGroupId(mceConsumerGroupId)
        .consumerSupplier(
            () -> createConsumerWithUniqueId(kafkaConsumerFactory, "trace-reader-mcp"))
        .pollDurationMs(pollDurationMs)
        .pollMaxAttempts(pollMaxAttempts)
        .timeoutSeconds(traceTimeoutSeconds)
        .executorService(traceExecutorService)
        .build();
  }

  @Bean("mcpFailedTraceReader")
  public MCPFailedTraceReader mcpFailedTraceReader(
      @Qualifier("traceAdminClient") AdminClient adminClient,
      @Qualifier("kafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      @Qualifier("traceExecutorService") ExecutorService traceExecutorService) {
    return MCPFailedTraceReader.builder()
        .adminClient(adminClient)
        .topicName(mcpFailedTopicName)
        .consumerSupplier(
            () -> createConsumerWithUniqueId(kafkaConsumerFactory, "trace-reader-mcp-failed"))
        .pollDurationMs(pollDurationMs)
        .pollMaxAttempts(pollMaxAttempts)
        .timeoutSeconds(traceTimeoutSeconds)
        .executorService(traceExecutorService)
        .build();
  }

  @Bean("mclVersionedTraceReader")
  public MCLTraceReader mclVersionedTraceReader(
      @Qualifier("traceAdminClient") AdminClient adminClient,
      @Qualifier("kafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      @Qualifier("traceExecutorService") ExecutorService traceExecutorService) {
    return MCLTraceReader.builder()
        .adminClient(adminClient)
        .topicName(mclVersionedTopicName)
        .consumerGroupId(maeConsumerGroupId)
        .consumerSupplier(
            () -> createConsumerWithUniqueId(kafkaConsumerFactory, "trace-reader-mcl-versioned"))
        .pollDurationMs(pollDurationMs)
        .pollMaxAttempts(pollMaxAttempts)
        .timeoutSeconds(traceTimeoutSeconds)
        .executorService(traceExecutorService)
        .build();
  }

  @Bean("mclTimeseriesTraceReader")
  public MCLTraceReader mclTimeseriesTraceReader(
      @Qualifier("traceAdminClient") AdminClient adminClient,
      @Qualifier("kafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      @Qualifier("traceExecutorService") ExecutorService traceExecutorService) {
    return MCLTraceReader.builder()
        .adminClient(adminClient)
        .topicName(mclTimeseriesTopicName)
        .consumerGroupId(maeConsumerGroupId)
        .consumerSupplier(
            () -> createConsumerWithUniqueId(kafkaConsumerFactory, "trace-reader-mcl-timeseries"))
        .pollDurationMs(pollDurationMs)
        .pollMaxAttempts(pollMaxAttempts)
        .timeoutSeconds(traceTimeoutSeconds)
        .executorService(traceExecutorService)
        .build();
  }

  private Consumer<String, GenericRecord> createConsumerWithUniqueId(
      DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      @Nonnull String baseClientId) {
    Properties consumerProps = new Properties();
    consumerProps.putAll(TRACE_CONSUMER_PROPERTIES);
    // Add a unique suffix to the client.id
    consumerProps.put(
        ConsumerConfig.CLIENT_ID_CONFIG,
        baseClientId + "-" + Thread.currentThread().getId() + "-" + System.nanoTime());

    return kafkaConsumerFactory.createConsumer(
        baseClientId, // groupId prefix
        null, // groupId suffix (using default)
        null, // assignor
        consumerProps);
  }

  @PreDestroy
  public void shutdown() {
    if (traceExecutorService != null) {
      traceExecutorService.shutdown();
      try {
        if (!traceExecutorService.awaitTermination(shutdownTimeoutSeconds, TimeUnit.SECONDS)) {
          traceExecutorService.shutdownNow();
          if (!traceExecutorService.awaitTermination(shutdownTimeoutSeconds, TimeUnit.SECONDS)) {
            System.err.println("ExecutorService did not terminate");
          }
        }
      } catch (InterruptedException e) {
        traceExecutorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }
}
