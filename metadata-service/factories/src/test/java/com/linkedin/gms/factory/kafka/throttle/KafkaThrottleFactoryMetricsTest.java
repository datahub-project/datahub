package com.linkedin.gms.factory.kafka.throttle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.datahub.metadata.dao.throttle.KafkaThrottleSensor;
import com.datahub.metadata.dao.throttle.PgQueueThrottleSensor;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.common.AdminClientFactory;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.mockito.MockedStatic;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.Test;

/**
 * Loads {@link KafkaThrottleFactory} with the production {@code application.yaml} and a {@link
 * MetricUtils} bean, throttles the versioned MCL topic, and checks the sensor's throttle gauge and
 * counter land in that bean's registry.
 */
public class KafkaThrottleFactoryMetricsTest {

  // MetricUtils caches meters by name in static maps, so each test uses its own topic names to
  // register fresh meters in its own registry.
  private static final String KAFKA_VERSIONED_TOPIC = "MetricsTest_Kafka_Versioned";
  private static final String KAFKA_TIMESERIES_TOPIC = "MetricsTest_Kafka_Timeseries";
  private static final String PGQUEUE_VERSIONED_TOPIC = "MetricsTest_PgQueue_Versioned";
  private static final String PGQUEUE_TIMESERIES_TOPIC = "MetricsTest_PgQueue_Timeseries";
  private static final String MAE_GROUP = "generic-mae-consumer-job-client";

  private static ApplicationContextRunner runner(
      SimpleMeterRegistry registry, String versionedTopic, String timeseriesTopic) {
    return new ApplicationContextRunner()
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withUserConfiguration(ConfigurationProvider.class, KafkaThrottleFactory.class)
        .withBean(
            "entityRegistry", EntityRegistry.class, TestOperationContexts::defaultEntityRegistry)
        .withBean(MetricUtils.class, () -> MetricUtils.builder().registry(registry).build())
        .withPropertyValues(
            "MCP_VERSIONED_THROTTLE_ENABLED=true",
            "MCP_VERSIONED_THRESHOLD=1",
            // the test calls throttle() itself; keep the sensor's own schedule from adding a count
            "MCP_THROTTLE_UPDATE_INTERVAL_MS=3600000",
            "METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME=" + versionedTopic,
            "METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME=" + timeseriesTopic);
  }

  @Test
  public void testKafkaSensorReportsThrottlingToMetricUtils() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    AdminClient admin = laggingAdmin();
    try (MockedStatic<AdminClientFactory> adminFactory = mockStatic(AdminClientFactory.class)) {
      adminFactory
          .when(() -> AdminClientFactory.buildKafkaAdminClient(any(), any(), anyString()))
          .thenReturn(admin);

      runner(registry, KAFKA_VERSIONED_TOPIC, KAFKA_TIMESERIES_TOPIC)
          .run(
              context -> {
                assertThat(context).hasNotFailed();
                KafkaThrottleSensor sensor =
                    context.getBean("kafkaThrottle", KafkaThrottleSensor.class);
                try {
                  sensor.refresh();
                  sensor.throttle();
                } finally {
                  sensor.stop();
                }
              });
    }

    assertThrottleMetrics(
        registry, KafkaThrottleSensor.class, KAFKA_VERSIONED_TOPIC, KAFKA_TIMESERIES_TOPIC);
  }

  @Test
  public void testPgQueueSensorReportsThrottlingToMetricUtils() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetadataQueueStore queueStore = mock(MetadataQueueStore.class);
    when(queueStore.fetchTopic(anyString())).thenReturn(Optional.empty());
    when(queueStore.fetchTopic(PGQUEUE_VERSIONED_TOPIC))
        .thenReturn(Optional.of(new QueueTopicMetadata(7L, 1, Optional.empty())));
    when(queueStore.partitionMaxEnqueueSeqs(7L, 1)).thenReturn(Map.of(0, 10L));
    when(queueStore.getCommittedOffset(MAE_GROUP, 7L, 0)).thenReturn(4L);

    runner(registry, PGQUEUE_VERSIONED_TOPIC, PGQUEUE_TIMESERIES_TOPIC)
        .withPropertyValues("datahub.messaging.transport=pgqueue")
        .withBean(MetadataQueueStore.class, () -> queueStore)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              PgQueueThrottleSensor sensor =
                  context.getBean("kafkaThrottle", PgQueueThrottleSensor.class);
              try {
                sensor.refresh();
                sensor.throttle();
              } finally {
                sensor.stop();
              }
            });

    assertThrottleMetrics(
        registry, PgQueueThrottleSensor.class, PGQUEUE_VERSIONED_TOPIC, PGQUEUE_TIMESERIES_TOPIC);
  }

  /** The versioned topic is throttled once; the timeseries topic (disabled) is not. */
  private static void assertThrottleMetrics(
      SimpleMeterRegistry registry,
      Class<?> sensorClass,
      String versionedTopic,
      String timeseriesTopic) {
    Gauge versionedThrottled =
        registry.find(meterName(sensorClass, versionedTopic + "_throttled")).gauge();
    assertThat(versionedThrottled).isNotNull();
    assertThat(versionedThrottled.value()).isEqualTo(1.0);

    Counter versionedThrottledCount =
        registry.find(meterName(sensorClass, versionedTopic + "_throttledCount")).counter();
    assertThat(versionedThrottledCount).isNotNull();
    assertThat(versionedThrottledCount.count()).isEqualTo(1.0);

    Gauge timeseriesThrottled =
        registry.find(meterName(sensorClass, timeseriesTopic + "_throttled")).gauge();
    assertThat(timeseriesThrottled).isNotNull();
    assertThat(timeseriesThrottled.value()).isEqualTo(0.0);
  }

  /** The name {@link MetricUtils} gives a per-class legacy meter. */
  private static String meterName(Class<?> sensorClass, String metricName) {
    return sensorClass.getName() + "." + metricName;
  }

  /** An admin client where the MAE group trails the versioned topic by 7, above the threshold. */
  private static AdminClient laggingAdmin() {
    TopicPartition versioned = new TopicPartition(KAFKA_VERSIONED_TOPIC, 0);
    TopicPartition timeseries = new TopicPartition(KAFKA_TIMESERIES_TOPIC, 0);
    AdminClient admin = mock(AdminClient.class);
    ListConsumerGroupOffsetsResult committed = mock(ListConsumerGroupOffsetsResult.class);
    when(committed.partitionsToOffsetAndMetadata())
        .thenReturn(
            KafkaFuture.completedFuture(
                Map.of(
                    versioned, new OffsetAndMetadata(10),
                    timeseries, new OffsetAndMetadata(20))));
    when(admin.listConsumerGroupOffsets(MAE_GROUP)).thenReturn(committed);
    ListOffsetsResult endOffsets = mock(ListOffsetsResult.class);
    when(endOffsets.all())
        .thenReturn(
            KafkaFuture.completedFuture(
                Map.of(
                    versioned, new ListOffsetsResultInfo(17, 0, Optional.empty()),
                    timeseries, new ListOffsetsResultInfo(23, 0, Optional.empty()))));
    when(admin.listOffsets(anyMap())).thenReturn(endOffsets);
    return admin;
  }
}
