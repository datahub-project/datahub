package com.linkedin.metadata.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.kafka.context.inbound.InboundContextResolver;
import com.linkedin.metadata.kafka.usage.UsageQueueIngestRecorder;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import org.apache.avro.generic.GenericRecord;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MetadataChangeProposalConsumerUsageRecorderTest {

  private MockedStatic<EventUtils> eventUtilsMock;
  private SystemEntityClient entityClient;
  private EventProducer eventProducer;
  private UsageQueueIngestRecorder usageQueueIngestRecorder;
  private MetadataChangeProposalConsumer consumer;
  private OperationContext systemContext;

  @BeforeMethod
  public void setUp() throws Exception {
    eventUtilsMock = Mockito.mockStatic(EventUtils.class);
    entityClient = mock(SystemEntityClient.class);
    eventProducer = mock(EventProducer.class);
    usageQueueIngestRecorder = mock(UsageQueueIngestRecorder.class);
    systemContext = TestOperationContexts.systemContextNoValidate();
    consumer =
        new MetadataChangeProposalConsumer(
            systemContext,
            entityClient,
            eventProducer,
            new InboundContextResolver(Collections.emptyList()));

    var field = MetadataChangeProposalConsumer.class.getDeclaredField("usageQueueIngestRecorder");
    field.setAccessible(true);
    field.set(consumer, usageQueueIngestRecorder);
  }

  @AfterMethod
  public void tearDown() {
    if (eventUtilsMock != null) {
      eventUtilsMock.close();
    }
  }

  @Test
  public void testInvokesUsageQueueIngestRecorderOnSuccessfulMcp() throws Exception {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityType("dataset");
    mcp.setChangeType(ChangeType.UPSERT);
    GenericRecord record = mock(GenericRecord.class);
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(record)).thenReturn(mcp);

    InboundMetadataEnvelope<GenericRecord> envelope =
        InboundMetadataEnvelope.<GenericRecord>builder()
            .logicalTopic("MetadataChangeProposal")
            .messagingSystem("kafka")
            .payload(record)
            .enqueuedAtMillis(System.currentTimeMillis())
            .kafkaPartition(1)
            .kafkaOffset(42L)
            .serializedValueSize(128)
            .build();

    consumer.accept(envelope, "MetadataChangeProposal-Consumer");

    verify(usageQueueIngestRecorder)
        .recordIfNeeded(eq(envelope), any(OperationContext.class), eq(mcp));
    verify(entityClient).ingestProposal(any(OperationContext.class), eq(mcp), eq(false));
  }
}
