package com.linkedin.metadata.pgqueue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.common.PgQueueConsumerInitializationManager;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.queue.MetadataQueueStore;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.testng.annotations.Test;

public class PgQueuePollWorkersBootstrapTest {

  private static PgQueueSetupOptions mockPgQueueOpts(int consumerConcurrency) {
    PgQueueSetupOptions opts = mock(PgQueueSetupOptions.class);
    when(opts.getTopicDefaultVisibilityTimeoutSeconds()).thenReturn(30);
    when(opts.getResolvedTopicCatalog()).thenReturn(List.of());
    when(opts.getTopicDefaultConsumerConcurrency()).thenReturn(consumerConcurrency);
    when(opts.getTopicDefaultPartitionCount()).thenReturn(4);
    return opts;
  }

  private static PostgresSqlSetupProperties mockPostgresProps(PgQueueSetupOptions opts) {
    PostgresSqlSetupProperties props = mock(PostgresSqlSetupProperties.class);
    when(props.buildPgQueueOptions(any())).thenReturn(opts);
    return props;
  }

  private static ConfigurationProvider mockConfigProvider() {
    ConfigurationProvider cp = mock(ConfigurationProvider.class);
    when(cp.getKafka()).thenReturn(mock(KafkaConfiguration.class));
    return cp;
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void constructorThrowsWhenPgQueueDisabled() {
    PostgresSqlSetupProperties props = mock(PostgresSqlSetupProperties.class);
    when(props.buildPgQueueOptions(any())).thenReturn(null);

    new PgQueuePollWorkersBootstrap(
        List.of(),
        mock(MetadataQueueStore.class),
        props,
        mockConfigProvider(),
        null,
        new PgQueueConsumerInitializationManager());
  }

  @Test
  public void startsWorkersAndReportsThreadHealth() throws Exception {
    PgQueueSetupOptions opts = mockPgQueueOpts(1);
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(any())).thenReturn(Optional.empty());

    PgQueuePollerRegistration reg =
        new PgQueuePollerRegistration(
            "cg-bootstrap",
            List.of("topic-a"),
            10,
            "worker-a",
            10,
            10,
            10,
            (topic, msgs, ctx) -> {});

    PgQueuePollerSource source = () -> Stream.of(reg);
    PgQueueConsumerInitializationManager lifecycle = new PgQueueConsumerInitializationManager();

    PgQueuePollWorkersBootstrap bootstrap =
        new PgQueuePollWorkersBootstrap(
            List.of(source), store, mockPostgresProps(opts), mockConfigProvider(), null, lifecycle);

    lifecycle.initialize("test");
    assertEquals(bootstrap.pollWorkerThreadCount(), 1);
    assertTrue(bootstrap.allPollWorkerThreadsAlive());

    bootstrap.destroy();
    Thread.sleep(50);
    assertFalse(bootstrap.allPollWorkerThreadsAlive());
  }

  @Test
  public void multiShardRegistrationUsesSuffixedThreadNames() {
    PgQueueSetupOptions opts = mockPgQueueOpts(2);
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(any())).thenReturn(Optional.empty());

    PgQueuePollerRegistration reg =
        new PgQueuePollerRegistration(
            "cg-sharded",
            List.of("topic-sharded"),
            10,
            "worker-sharded",
            10,
            10,
            10,
            (topic, msgs, ctx) -> {});

    PgQueueConsumerInitializationManager lifecycle = new PgQueueConsumerInitializationManager();
    PgQueuePollWorkersBootstrap bootstrap =
        new PgQueuePollWorkersBootstrap(
            List.of(() -> Stream.of(reg)),
            store,
            mockPostgresProps(opts),
            mockConfigProvider(),
            null,
            lifecycle);

    lifecycle.initialize("test");
    assertEquals(bootstrap.pollWorkerThreadCount(), 2);

    bootstrap.destroy();
  }

  @Test
  public void allPollWorkerThreadsAliveTrueWhenNoThreadsStarted() {
    PgQueueSetupOptions opts = mockPgQueueOpts(1);
    PgQueueConsumerInitializationManager lifecycle = new PgQueueConsumerInitializationManager();

    PgQueuePollWorkersBootstrap bootstrap =
        new PgQueuePollWorkersBootstrap(
            List.of(),
            mock(MetadataQueueStore.class),
            mockPostgresProps(opts),
            mockConfigProvider(),
            null,
            lifecycle);

    assertEquals(bootstrap.pollWorkerThreadCount(), 0);
    assertTrue(bootstrap.allPollWorkerThreadsAlive());
  }
}
