package com.linkedin.metadata.kafka.config;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.MceConsumerConfiguration;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.kafka.MetadataChangeProposalConsumer;
import com.linkedin.metadata.kafka.batch.BatchMetadataChangeProposalsProcessor;
import com.linkedin.metadata.pgqueue.PgQueuePollContext;
import com.linkedin.metadata.pgqueue.PgQueuePollerRegistration;
import com.linkedin.metadata.pgqueue.PgQueuePollerSource;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PgQueueMcePollerSourcesConfigurationTest {

  private ConfigurationProvider configurationProvider;
  private PostgresSqlSetupProperties postgresSqlSetupProperties;
  private PgQueueMcePollerSourcesConfiguration configuration;

  @BeforeMethod
  public void setUp() {
    configurationProvider = mock(ConfigurationProvider.class);
    MceConsumerConfiguration.PgQueuePoll poll = new MceConsumerConfiguration.PgQueuePoll();
    poll.setMetadataChangeProposalMaxBatch(50);
    poll.setBatchMetadataChangeProposalMaxBatch(100);
    MceConsumerConfiguration mceConsumer = new MceConsumerConfiguration();
    mceConsumer.setPgQueue(poll);
    when(configurationProvider.getMceConsumer()).thenReturn(mceConsumer);
    postgresSqlSetupProperties = samplePostgresPollConfig();
    configuration =
        new PgQueueMcePollerSourcesConfiguration(configurationProvider, postgresSqlSetupProperties);
  }

  private static PostgresSqlSetupProperties samplePostgresPollConfig() {
    PostgresSqlSetupProperties properties = new PostgresSqlSetupProperties();
    PostgresSqlSetupProperties.PgQueue.ConsumerPoll consumerPoll =
        new PostgresSqlSetupProperties.PgQueue.ConsumerPoll();
    consumerPoll.setEmptyPollSleepMillis(100L);
    consumerPoll.setMissingTopicSleepMillis(500L);
    consumerPoll.setErrorRecoverySleepMillis(1000L);
    properties.getPgQueue().setConsumerPoll(consumerPoll);
    return properties;
  }

  @Test
  public void pgQueueMcpSource_registersMcpPoller() {
    MetadataChangeProposalConsumer consumer = mock(MetadataChangeProposalConsumer.class);

    PgQueuePollerSource source =
        configuration.pgQueueMcpSource(consumer, "mce-group", Topics.METADATA_CHANGE_PROPOSAL);

    PgQueuePollerRegistration reg = source.registrations().collect(Collectors.toList()).get(0);
    assertEquals(reg.consumerGroupId(), "mce-group");
    assertEquals(reg.topicNames(), List.of(Topics.METADATA_CHANGE_PROPOSAL));
    assertEquals(reg.maxBatch(), 50);
    assertNotNull(reg.handler());
  }

  @Test
  public void pgQueueMcpSource_usesConfiguredPollMaxBatch() {
    configurationProvider.getMceConsumer().getPgQueue().setMetadataChangeProposalMaxBatch(75);

    assertEquals(configuration.metadataChangeProposalPollMaxBatch(), 75);
  }

  @Test
  public void pgQueueMcpSource_handlerAcceptsAndCommits() throws Exception {
    MetadataChangeProposalConsumer consumer = mock(MetadataChangeProposalConsumer.class);
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);
    GenericRecord record = mock(GenericRecord.class);
    when(deserializer.deserialize(anyString(), any(byte[].class))).thenReturn(record);

    PgQueuePollerRegistration reg =
        configuration
            .pgQueueMcpSource(consumer, "mce-group", Topics.METADATA_CHANGE_PROPOSAL)
            .registrations()
            .findFirst()
            .orElseThrow();

    QueueReceivedMessage msg = sampleMessage();
    PgQueuePollContext ctx =
        new PgQueuePollContext(store, "mce-group", Duration.ofSeconds(30), deserializer);
    reg.handler().handleBatch(Topics.METADATA_CHANGE_PROPOSAL, List.of(msg), ctx);

    verify(consumer).accept(any(), eq("mce-group"));
    verify(store).commitForGroup(eq("mce-group"), eq(List.of(msg.handle())), eq(true));
  }

  @Test
  public void pgQueueBatchMcpSource_handlerBuildsSyntheticRecords() throws Exception {
    BatchMetadataChangeProposalsProcessor batchProcessor =
        mock(BatchMetadataChangeProposalsProcessor.class);
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);
    when(deserializer.deserialize(anyString(), any(byte[].class)))
        .thenReturn(mock(GenericRecord.class));

    PgQueuePollerRegistration reg =
        configuration
            .pgQueueBatchMcpSource(
                batchProcessor,
                mock(OperationContext.class),
                "batch-group",
                Topics.METADATA_CHANGE_PROPOSAL)
            .registrations()
            .findFirst()
            .orElseThrow();

    QueueReceivedMessage msg = sampleMessage();
    PgQueuePollContext ctx =
        new PgQueuePollContext(store, "batch-group", Duration.ofSeconds(30), deserializer);
    reg.handler().handleBatch(Topics.METADATA_CHANGE_PROPOSAL, List.of(msg), ctx);

    verify(batchProcessor).consume(any(OperationContext.class), any(), any());
    verify(store).commitForGroup(eq("batch-group"), eq(List.of(msg.handle())), eq(true));
  }

  @Test
  public void pgQueueBatchMcpSource_registersBatchPoller() {
    BatchMetadataChangeProposalsProcessor batchProcessor =
        mock(BatchMetadataChangeProposalsProcessor.class);

    PgQueuePollerSource source =
        configuration.pgQueueBatchMcpSource(
            batchProcessor,
            mock(OperationContext.class),
            "batch-group",
            Topics.METADATA_CHANGE_PROPOSAL);

    PgQueuePollerRegistration reg = source.registrations().collect(Collectors.toList()).get(0);
    assertEquals(reg.consumerGroupId(), "batch-group");
    assertEquals(reg.maxBatch(), 100);
    assertEquals(reg.threadName(), "pgqueue-batch-mcp-batch-group");
    assertNotNull(reg.handler());
  }

  @Test
  public void pgQueueBatchMcpSource_usesConfiguredPollMaxBatch() {
    configurationProvider.getMceConsumer().getPgQueue().setBatchMetadataChangeProposalMaxBatch(150);

    assertEquals(configuration.batchMetadataChangeProposalPollMaxBatch(), 150);
  }

  private static QueueReceivedMessage sampleMessage() {
    return sampleMessage(new byte[] {1, 2, 3});
  }

  private static QueueReceivedMessage sampleMessage(byte[] payload) {
    QueueMessageHandle handle = new QueueMessageHandle(1L, Instant.EPOCH, 0, 0, 1L);
    return new QueueReceivedMessage(
        handle,
        0,
        payload,
        Optional.empty(),
        PgQueuePayloadCompression.NONE,
        List.of(),
        "key",
        "owner");
  }
}
