package com.datahub.event.config;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.datahub.event.PlatformEventProcessor;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.PeConsumerConfiguration;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.pgqueue.PgQueuePollContext;
import com.linkedin.metadata.pgqueue.PgQueuePollerRegistration;
import com.linkedin.metadata.pgqueue.PgQueuePollerSource;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.mxe.Topics;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PgQueuePePollerSourcesConfigurationTest {

  private ConfigurationProvider configurationProvider;
  private PgQueuePePollerSourcesConfiguration configuration;

  @BeforeMethod
  public void setUp() {
    configurationProvider = mock(ConfigurationProvider.class);
    PeConsumerConfiguration.PgQueuePoll poll = new PeConsumerConfiguration.PgQueuePoll();
    poll.setPlatformEventMaxBatch(50);
    PeConsumerConfiguration peConsumer = new PeConsumerConfiguration();
    peConsumer.setPgQueue(poll);
    when(configurationProvider.getPeConsumer()).thenReturn(peConsumer);

    PostgresSqlSetupProperties postgresSqlSetupProperties = new PostgresSqlSetupProperties();
    PostgresSqlSetupProperties.PgQueue.ConsumerPoll consumerPoll =
        new PostgresSqlSetupProperties.PgQueue.ConsumerPoll();
    consumerPoll.setEmptyPollSleepMillis(100L);
    consumerPoll.setMissingTopicSleepMillis(500L);
    consumerPoll.setErrorRecoverySleepMillis(1000L);
    postgresSqlSetupProperties.getPgQueue().setConsumerPoll(consumerPoll);

    configuration =
        new PgQueuePePollerSourcesConfiguration(configurationProvider, postgresSqlSetupProperties);
  }

  @Test
  public void pgQueuePeSource_registersPoller() {
    PlatformEventProcessor processor = mock(PlatformEventProcessor.class);

    PgQueuePollerSource source =
        configuration.pgQueuePeSource(processor, Topics.PLATFORM_EVENT, "pe-consumer-group");

    PgQueuePollerRegistration reg = source.registrations().collect(Collectors.toList()).get(0);
    assertEquals(reg.consumerGroupId(), "pe-consumer-group");
    assertEquals(reg.topicNames(), List.of(Topics.PLATFORM_EVENT));
    assertEquals(reg.maxBatch(), 50);
    assertNotNull(reg.handler());
  }

  @Test
  public void pgQueuePeSource_handlerConsumesAndCommits() throws Exception {
    PlatformEventProcessor processor = mock(PlatformEventProcessor.class);
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);
    when(deserializer.deserialize(anyString(), any(byte[].class)))
        .thenReturn(mock(GenericRecord.class));

    PgQueuePollerRegistration reg =
        configuration
            .pgQueuePeSource(processor, "PlatformEvent_v1", "pe-group")
            .registrations()
            .findFirst()
            .orElseThrow();

    QueueMessageHandle handle = new QueueMessageHandle(1L, Instant.EPOCH, 0, 0, 1L);
    QueueReceivedMessage msg =
        new QueueReceivedMessage(
            handle,
            0,
            new byte[] {1},
            Optional.empty(),
            PgQueuePayloadCompression.NONE,
            List.of(),
            "key",
            "owner");
    PgQueuePollContext ctx =
        new PgQueuePollContext(store, "pe-group", Duration.ofSeconds(30), deserializer);
    reg.handler().handleBatch("PlatformEvent_v1", List.of(msg), ctx);

    verify(processor).consumeEnvelope(any());
    verify(store).commitForGroup(eq("pe-group"), eq(List.of(handle)), eq(true));
  }
}
