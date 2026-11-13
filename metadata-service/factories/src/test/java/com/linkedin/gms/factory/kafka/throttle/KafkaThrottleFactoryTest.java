package com.linkedin.gms.factory.kafka.throttle;

import static com.linkedin.metadata.dao.throttle.ThrottleType.MCL_TIMESERIES_LAG;
import static com.linkedin.metadata.dao.throttle.ThrottleType.MCL_VERSIONED_LAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datahub.metadata.dao.throttle.KafkaThrottleSensor;
import com.datahub.metadata.dao.throttle.PgQueueThrottleSensor;
import com.linkedin.gms.factory.ProductionConfigContextRunner;
import com.linkedin.gms.factory.kafka.common.AdminClientFactory;
import com.linkedin.metadata.dao.throttle.NoOpSensor;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import io.datahubproject.test.metadata.context.TestOperationContexts;
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
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.testng.annotations.Test;

/**
 * Loads {@link KafkaThrottleFactory} with the production {@code application.yaml} and checks which
 * {@link ThrottleSensor} the {@code kafkaThrottle} bean is, given the update interval, the
 * messaging transport and the Kafka bootstrap settings. The Kafka admin client and the queue store
 * are the only mocks.
 */
public class KafkaThrottleFactoryTest {

  private static final String DEFAULT_MAE_GROUP = "generic-mae-consumer-job-client";
  private static final String DEFAULT_VERSIONED_TOPIC = "MetadataChangeLog_Versioned_v1";
  private static final String DEFAULT_TIMESERIES_TOPIC = "MetadataChangeLog_Timeseries_v1";

  private static ApplicationContextRunner runner() {
    return ProductionConfigContextRunner.create()
        .withUserConfiguration(KafkaThrottleFactory.class)
        .withBean(
            "entityRegistry", EntityRegistry.class, TestOperationContexts::defaultEntityRegistry);
  }

  @Test
  public void testProductionDefaultsBuildAKafkaSensorOnTheMaeConsumerGroup() throws Exception {
    AdminClient admin = laggingAdmin(DEFAULT_MAE_GROUP);
    try (MockedStatic<AdminClientFactory> adminFactory = mockStatic(AdminClientFactory.class)) {
      adminFactory
          .when(() -> AdminClientFactory.buildKafkaAdminClient(any(), any(), anyString()))
          .thenReturn(admin);

      runner()
          .run(
              context -> {
                assertThat(context).hasNotFailed();
                KafkaThrottleSensor sensor =
                    context.getBean("kafkaThrottle", KafkaThrottleSensor.class);
                try {
                  sensor.refresh();
                  verify(admin).listConsumerGroupOffsets(DEFAULT_MAE_GROUP);
                  assertThat(sensor.getLag())
                      .containsEntry(MCL_VERSIONED_LAG, 7L)
                      .containsEntry(MCL_TIMESERIES_LAG, 3L);
                } finally {
                  sensor.stop();
                }
              });

      adminFactory.verify(
          () -> AdminClientFactory.buildKafkaAdminClient(any(), any(), eq("throttle-sensor")));
    }
  }

  @Test
  public void testConsumerGroupOverrideReachesTheKafkaSensor() throws Exception {
    AdminClient admin = laggingAdmin("custom-mae-group");
    try (MockedStatic<AdminClientFactory> adminFactory = mockStatic(AdminClientFactory.class)) {
      adminFactory
          .when(() -> AdminClientFactory.buildKafkaAdminClient(any(), any(), anyString()))
          .thenReturn(admin);

      runner()
          .withPropertyValues("METADATA_CHANGE_LOG_KAFKA_CONSUMER_GROUP_ID=custom-mae-group")
          .run(
              context -> {
                KafkaThrottleSensor sensor =
                    context.getBean("kafkaThrottle", KafkaThrottleSensor.class);
                try {
                  sensor.refresh();
                  verify(admin).listConsumerGroupOffsets("custom-mae-group");
                } finally {
                  sensor.stop();
                }
              });
    }
  }

  @Test
  public void testNonPositiveUpdateIntervalDisablesThrottling() {
    runner()
        .withPropertyValues("metadataChangeProposal.throttle.updateIntervalMs=0")
        .run(context -> assertThat(sensor(context)).isInstanceOf(NoOpSensor.class));
  }

  @Test
  public void testPgQueueTransportReadsLagFromTheQueueStore() {
    MetadataQueueStore queueStore = mock(MetadataQueueStore.class);
    when(queueStore.fetchTopic(anyString())).thenReturn(Optional.empty());
    when(queueStore.fetchTopic(DEFAULT_VERSIONED_TOPIC))
        .thenReturn(Optional.of(new QueueTopicMetadata(7L, 1, Optional.empty())));
    when(queueStore.partitionMaxEnqueueSeqs(7L, 1)).thenReturn(Map.of(0, 10L));
    when(queueStore.getCommittedOffset(DEFAULT_MAE_GROUP, 7L, 0)).thenReturn(4L);

    runner()
        .withPropertyValues("datahub.messaging.transport=pgqueue")
        .withBean(MetadataQueueStore.class, () -> queueStore)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              PgQueueThrottleSensor sensor =
                  context.getBean("kafkaThrottle", PgQueueThrottleSensor.class);
              try {
                sensor.refresh();
                verify(queueStore).fetchTopic(DEFAULT_VERSIONED_TOPIC);
                verify(queueStore).fetchTopic(DEFAULT_TIMESERIES_TOPIC);
                verify(queueStore).getCommittedOffset(DEFAULT_MAE_GROUP, 7L, 0);
                assertThat(sensor.getLag())
                    .containsEntry(MCL_VERSIONED_LAG, 6L)
                    .containsEntry(MCL_TIMESERIES_LAG, 0L);
              } finally {
                sensor.stop();
              }
            });
  }

  @Test
  public void testPgQueueTransportWithoutAQueueStoreFailsStartup() {
    runner()
        .withPropertyValues("datahub.messaging.transport=pgqueue")
        .run(
            context ->
                assertThat(context)
                    .hasFailed()
                    .getFailure()
                    .rootCause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("MetadataQueueStore"));
  }

  @Test
  public void testOtherTransportsUseNoOpSensor() {
    runner()
        .withPropertyValues("datahub.messaging.transport=none")
        .run(context -> assertThat(sensor(context)).isInstanceOf(NoOpSensor.class));
  }

  @Test
  public void testNoResolvableBootstrapServersUsesNoOpSensor() {
    try (MockedStatic<AdminClientFactory> adminFactory = mockStatic(AdminClientFactory.class)) {
      runner()
          .withPropertyValues(
              "kafka.bootstrapServers=",
              "kafka.producer.bootstrapServers=",
              "spring.kafka.bootstrap-servers=")
          .run(context -> assertThat(sensor(context)).isInstanceOf(NoOpSensor.class));

      adminFactory.verifyNoInteractions();
    }
  }

  private static ThrottleSensor sensor(ApplicationContext context) {
    return context.getBean("kafkaThrottle", ThrottleSensor.class);
  }

  /**
   * An admin client where the given group trails the versioned topic by 7 and the timeseries topic
   * by 3. Each topic needs a committed offset: with none, the sensor's median is undefined.
   */
  private static AdminClient laggingAdmin(String groupId) {
    TopicPartition versioned = new TopicPartition(DEFAULT_VERSIONED_TOPIC, 0);
    TopicPartition timeseries = new TopicPartition(DEFAULT_TIMESERIES_TOPIC, 0);
    AdminClient admin = mock(AdminClient.class);
    ListConsumerGroupOffsetsResult committed = mock(ListConsumerGroupOffsetsResult.class);
    when(committed.partitionsToOffsetAndMetadata())
        .thenReturn(
            KafkaFuture.completedFuture(
                Map.of(
                    versioned, new OffsetAndMetadata(10),
                    timeseries, new OffsetAndMetadata(20))));
    when(admin.listConsumerGroupOffsets(groupId)).thenReturn(committed);
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
