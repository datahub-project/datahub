package com.linkedin.gms.factory.kafka.trace;

import com.linkedin.metadata.config.messaging.KafkaMessagingEnabled;
import com.linkedin.metadata.trace.EphemeralTraceConsumerPool;
import com.linkedin.metadata.trace.TraceConsumerPool;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.event.kafka.KafkaConsumerPool;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
@KafkaMessagingEnabled
public class TraceKafkaConsumerPoolFactory {

  private static final String LEAVE_GROUP_ON_CLOSE_CONFIG = "internal.leave.group.on.close";

  private static final Map<String, Object> TRACE_CONSUMER_PROPERTIES =
      Map.of(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

  @Value("${trace.kafka.consumerPool.enabled:true}")
  private boolean poolEnabled;

  @Value("${trace.kafka.consumerPool.initialSize:4}")
  private int initialPoolSize;

  @Value("${trace.kafka.consumerPool.maxSize:16}")
  private int maxPoolSize;

  @Value("${trace.kafka.consumerPool.borrowTimeoutMs:5000}")
  private long borrowTimeoutMs;

  @Value("${trace.kafka.consumerPool.validationTimeoutSeconds:5}")
  private int validationTimeoutSeconds;

  @Value("${trace.kafka.consumerPool.validationCacheIntervalMinutes:5}")
  private int validationCacheIntervalMinutes;

  @Value("${trace.kafka.consumerPool.leaveGroupOnClose:false}")
  private boolean leaveGroupOnClose;

  @Value("${trace.kafka.consumerPool.groupId.mcp:trace-reader-mcp}")
  private String mcpGroupId;

  @Value("${trace.kafka.consumerPool.groupId.mcpFailed:trace-reader-mcp-failed}")
  private String mcpFailedGroupId;

  @Value("${trace.kafka.consumerPool.groupId.mclVersioned:trace-reader-mcl-versioned}")
  private String mclVersionedGroupId;

  @Value("${trace.kafka.consumerPool.groupId.mclTimeseries:trace-reader-mcl-timeseries}")
  private String mclTimeseriesGroupId;

  @Autowired(required = false)
  private MetricUtils metricUtils;

  private final Map<String, TraceConsumerPool> pools = new ConcurrentHashMap<>();

  @Bean("mcpTraceConsumerPool")
  public TraceConsumerPool mcpTraceConsumerPool(
      @Qualifier("kafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory) {
    return createPool("mcp", mcpGroupId, kafkaConsumerFactory);
  }

  @Bean("mcpFailedTraceConsumerPool")
  public TraceConsumerPool mcpFailedTraceConsumerPool(
      @Qualifier("kafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory) {
    return createPool("mcpFailed", mcpFailedGroupId, kafkaConsumerFactory);
  }

  @Bean("mclVersionedTraceConsumerPool")
  public TraceConsumerPool mclVersionedTraceConsumerPool(
      @Qualifier("kafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory) {
    return createPool("mclVersioned", mclVersionedGroupId, kafkaConsumerFactory);
  }

  @Bean("mclTimeseriesTraceConsumerPool")
  public TraceConsumerPool mclTimeseriesTraceConsumerPool(
      @Qualifier("kafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory) {
    return createPool("mclTimeseries", mclTimeseriesGroupId, kafkaConsumerFactory);
  }

  private TraceConsumerPool createPool(
      String poolType,
      String groupId,
      DefaultKafkaConsumerFactory<String, GenericRecord> baseConsumerFactory) {
    validatePoolSize();
    TraceConsumerPool pool;
    if (poolEnabled) {
      DefaultKafkaConsumerFactory<String, GenericRecord> traceConsumerFactory =
          createTraceConsumerFactory(baseConsumerFactory, groupId);
      KafkaConsumerPool kafkaConsumerPool =
          new KafkaConsumerPool(
              traceConsumerFactory,
              initialPoolSize,
              maxPoolSize,
              Duration.ofSeconds(validationTimeoutSeconds),
              Duration.ofMinutes(validationCacheIntervalMinutes),
              metricUtils);
      pool = new KafkaTraceConsumerPool(kafkaConsumerPool, borrowTimeoutMs, poolType, metricUtils);
    } else {
      pool =
          new EphemeralTraceConsumerPool(
              () -> createEphemeralConsumer(baseConsumerFactory, groupId));
    }
    pools.put(poolType, pool);
    return pool;
  }

  @Nonnull
  DefaultKafkaConsumerFactory<String, GenericRecord> createTraceConsumerFactory(
      DefaultKafkaConsumerFactory<String, GenericRecord> baseConsumerFactory, String groupId) {
    Map<String, Object> props = new HashMap<>(baseConsumerFactory.getConfigurationProperties());
    props.putAll(TRACE_CONSUMER_PROPERTIES);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(LEAVE_GROUP_ON_CLOSE_CONFIG, leaveGroupOnClose);
    return new DefaultKafkaConsumerFactory<>(
        props,
        baseConsumerFactory.getKeyDeserializer(),
        baseConsumerFactory.getValueDeserializer());
  }

  private Consumer<String, GenericRecord> createEphemeralConsumer(
      DefaultKafkaConsumerFactory<String, GenericRecord> baseConsumerFactory, String groupId) {
    Properties consumerProps = new Properties();
    consumerProps.putAll(TRACE_CONSUMER_PROPERTIES);
    consumerProps.put(LEAVE_GROUP_ON_CLOSE_CONFIG, Boolean.toString(leaveGroupOnClose));
    consumerProps.put(
        ConsumerConfig.CLIENT_ID_CONFIG,
        groupId + "-" + Thread.currentThread().getId() + "-" + System.nanoTime());
    return baseConsumerFactory.createConsumer(groupId, null, null, consumerProps);
  }

  private void validatePoolSize() {
    if (initialPoolSize > maxPoolSize) {
      throw new IllegalArgumentException(
          "trace.kafka.consumerPool.initialSize ("
              + initialPoolSize
              + ") must not exceed trace.kafka.consumerPool.maxSize ("
              + maxPoolSize
              + ")");
    }
  }

  @PreDestroy
  public void shutdown() {
    pools.values().forEach(TraceConsumerPool::shutdown);
  }
}
