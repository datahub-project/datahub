package com.linkedin.gms.factory.kafka.throttle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datahub.metadata.dao.throttle.KafkaThrottleSensor;
import com.datahub.metadata.dao.throttle.PgQueueThrottleSensor;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.common.AdminClientFactory;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.mxe.Topics;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Duration;
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
 * Loads {@link KafkaThrottleFactory} with the production {@code application.yaml}, versioned
 * throttling on and a 10ms refresh, and checks that closing the context stops the sensor's lag
 * refresh and closes the Kafka admin client it owns.
 */
public class KafkaThrottleFactoryShutdownTest {

  private static final long REFRESH_INTERVAL_MS = 10;

  private static ApplicationContextRunner runner() {
    return new ApplicationContextRunner()
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withUserConfiguration(ConfigurationProvider.class, KafkaThrottleFactory.class)
        .withBean(
            "entityRegistry", EntityRegistry.class, TestOperationContexts::defaultEntityRegistry)
        .withPropertyValues(
            "MCP_VERSIONED_THROTTLE_ENABLED=true",
            "MCP_THROTTLE_UPDATE_INTERVAL_MS=" + REFRESH_INTERVAL_MS);
  }

  @Test
  public void testContextCloseStopsKafkaSensorAndClosesAdminClient() throws Exception {
    AdminClient admin = idleAdmin();
    try (MockedStatic<AdminClientFactory> adminFactory = mockStatic(AdminClientFactory.class)) {
      adminFactory
          .when(() -> AdminClientFactory.buildKafkaAdminClient(any(), any(), anyString()))
          .thenReturn(admin);

      runner()
          .run(
              context -> {
                assertThat(context).hasNotFailed();
                assertThat(context.getBean("kafkaThrottle"))
                    .isInstanceOf(KafkaThrottleSensor.class);
                // the scheduled refresh is running
                verify(admin, timeout(5_000).atLeastOnce()).listConsumerGroupOffsets(anyString());
                verify(admin, never()).close(any(Duration.class));
              });
    }

    verify(admin).close(any(Duration.class));
    assertNoMoreRefreshes(admin);
  }

  @Test
  public void testContextCloseStopsPgQueueSensor() throws Exception {
    MetadataQueueStore queueStore = mock(MetadataQueueStore.class);
    when(queueStore.fetchTopic(anyString())).thenReturn(Optional.empty());

    runner()
        .withPropertyValues("datahub.messaging.transport=pgqueue")
        .withBean(MetadataQueueStore.class, () -> queueStore)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean("kafkaThrottle"))
                  .isInstanceOf(PgQueueThrottleSensor.class);
              verify(queueStore, timeout(5_000).atLeastOnce()).fetchTopic(anyString());
            });

    assertNoMoreRefreshes(queueStore);
  }

  /**
   * close() waits for a refresh in flight, so once the context is closed the mock must see no new
   * calls over many refresh intervals.
   */
  private static void assertNoMoreRefreshes(Object refreshSource) throws InterruptedException {
    int callsAfterClose = mockingDetails(refreshSource).getInvocations().size();
    Thread.sleep(REFRESH_INTERVAL_MS * 20);
    assertThat(mockingDetails(refreshSource).getInvocations())
        .as("lag refreshes after the context closed")
        .hasSize(callsAfterClose);
  }

  /** An admin client where the MAE group is caught up on both MCL topics. */
  private static AdminClient idleAdmin() {
    TopicPartition versioned = new TopicPartition(Topics.METADATA_CHANGE_LOG_VERSIONED, 0);
    TopicPartition timeseries = new TopicPartition(Topics.METADATA_CHANGE_LOG_TIMESERIES, 0);
    AdminClient admin = mock(AdminClient.class);
    ListConsumerGroupOffsetsResult committed = mock(ListConsumerGroupOffsetsResult.class);
    when(committed.partitionsToOffsetAndMetadata())
        .thenReturn(
            KafkaFuture.completedFuture(
                Map.of(
                    versioned, new OffsetAndMetadata(10), timeseries, new OffsetAndMetadata(20))));
    when(admin.listConsumerGroupOffsets(anyString())).thenReturn(committed);
    ListOffsetsResult endOffsets = mock(ListOffsetsResult.class);
    when(endOffsets.all())
        .thenReturn(
            KafkaFuture.completedFuture(
                Map.of(
                    versioned, new ListOffsetsResultInfo(10, 0, Optional.empty()),
                    timeseries, new ListOffsetsResultInfo(20, 0, Optional.empty()))));
    when(admin.listOffsets(anyMap())).thenReturn(endOffsets);
    return admin;
  }
}
