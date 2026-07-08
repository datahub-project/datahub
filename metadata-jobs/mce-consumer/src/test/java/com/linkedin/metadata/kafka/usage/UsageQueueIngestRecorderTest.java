package com.linkedin.metadata.kafka.usage;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.kafka.InboundMetadataEnvelope;
import com.linkedin.metadata.usage.UsageDedupHeaders;
import com.linkedin.metadata.usage.flush.FlushTrigger;
import com.linkedin.metadata.usage.store.UsageAggregationStore;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UsageQueueIngestRecorderTest {

  @Test
  public void testSkipsWhenPreRecorded() {
    AtomicInteger count = new AtomicInteger();
    UsageQueueIngestRecorder recorder = new UsageQueueIngestRecorder(countingStore(count));
    InboundMetadataEnvelope<String> envelope =
        InboundMetadataEnvelope.<String>builder()
            .logicalTopic("MetadataChangeProposal")
            .messagingSystem("kafka")
            .payload("x")
            .enqueuedAtMillis(1L)
            .build();
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    UsageDedupHeaders.stampPreRecorded(mcp);
    OperationContext context =
        TestOperationContexts.systemContextNoValidate().toBuilder()
            .build(new Authentication(new Actor(ActorType.USER, "system"), ""), true);

    recorder.recordIfNeeded(envelope, context, mcp);

    Assert.assertEquals(count.get(), 0);
  }

  @Test
  public void testRecordsMessagingMetadataIngest() {
    AtomicInteger count = new AtomicInteger();
    UsageQueueIngestRecorder recorder = new UsageQueueIngestRecorder(countingStore(count));
    InboundMetadataEnvelope<String> envelope =
        InboundMetadataEnvelope.<String>builder()
            .logicalTopic("MetadataChangeProposal")
            .messagingSystem("kafka")
            .payload("x")
            .enqueuedAtMillis(1L)
            .kafkaPartition(0)
            .kafkaOffset(99L)
            .serializedValueSize(512)
            .build();
    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setAspectModified(
        new AuditStamp().setTime(1L).setActor(UrnUtils.getUrn("urn:li:corpuser:ingest-actor")));
    MetadataChangeProposal mcp = new MetadataChangeProposal().setSystemMetadata(systemMetadata);
    OperationContext context =
        TestOperationContexts.systemContextNoValidate().toBuilder()
            .build(new Authentication(new Actor(ActorType.USER, "system"), ""), true);

    recorder.recordIfNeeded(envelope, context, mcp);

    Assert.assertEquals(count.get(), 1);
  }

  @Test
  public void testRecordsWithPgQueueEnqueueSeqRequestId() {
    AtomicInteger count = new AtomicInteger();
    UsageQueueIngestRecorder recorder = new UsageQueueIngestRecorder(simpleCountingStore(count));
    InboundMetadataEnvelope<String> envelope =
        InboundMetadataEnvelope.<String>builder()
            .logicalTopic("MetadataChangeProposal")
            .messagingSystem("pgqueue")
            .payload("x")
            .enqueuedAtMillis(1L)
            .enqueueSeq(7L)
            .build();
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    OperationContext context =
        TestOperationContexts.systemContextNoValidate().toBuilder()
            .build(new Authentication(new Actor(ActorType.USER, "system"), ""), true);

    recorder.recordIfNeeded(envelope, context, mcp);

    Assert.assertEquals(count.get(), 1);
  }

  @Test
  public void testRecordsWithTopicOnlyRequestId() {
    AtomicInteger count = new AtomicInteger();
    UsageQueueIngestRecorder recorder = new UsageQueueIngestRecorder(simpleCountingStore(count));
    InboundMetadataEnvelope<String> envelope =
        InboundMetadataEnvelope.<String>builder()
            .logicalTopic("MetadataChangeProposal")
            .messagingSystem("kafka")
            .payload("x")
            .enqueuedAtMillis(1L)
            .build();
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    OperationContext context =
        TestOperationContexts.systemContextNoValidate().toBuilder()
            .build(new Authentication(new Actor(ActorType.USER, "system"), ""), true);

    recorder.recordIfNeeded(envelope, context, mcp);

    Assert.assertEquals(count.get(), 1);
  }

  @Test
  public void testUsesSessionActorWhenSystemMetadataAbsent() {
    AtomicInteger count = new AtomicInteger();
    UsageQueueIngestRecorder recorder =
        new UsageQueueIngestRecorder(assertingStore(count, "urn:li:corpuser:session-actor"));
    InboundMetadataEnvelope<String> envelope = minimalEnvelope();
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    OperationContext context =
        TestOperationContexts.systemContextNoValidate().toBuilder()
            .build(new Authentication(new Actor(ActorType.USER, "session-actor"), ""), true);

    recorder.recordIfNeeded(envelope, context, mcp);

    Assert.assertEquals(count.get(), 1);
  }

  @Test
  public void testHandlesNullSessionActorGracefully() {
    AtomicInteger count = new AtomicInteger();
    UsageQueueIngestRecorder recorder = new UsageQueueIngestRecorder(simpleCountingStore(count));
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    OperationContext base = TestOperationContexts.systemContextNoValidate();
    OperationContext context = mock(OperationContext.class);
    Authentication auth = mock(Authentication.class);
    when(auth.getActor()).thenReturn(null);
    when(context.getSessionAuthentication()).thenReturn(auth);
    when(context.toBuilder()).thenReturn(base.toBuilder());

    recorder.recordIfNeeded(minimalEnvelope(), context, mcp);

    Assert.assertEquals(count.get(), 0);
  }

  @Test
  public void testFallsBackToSessionActorWhenSystemActorInAspectModified() {
    AtomicInteger count = new AtomicInteger();
    UsageQueueIngestRecorder recorder =
        new UsageQueueIngestRecorder(assertingStore(count, "urn:li:corpuser:session"));
    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setAspectModified(
        new AuditStamp()
            .setTime(1L)
            .setActor(UrnUtils.getUrn(com.linkedin.metadata.Constants.SYSTEM_ACTOR)));
    MetadataChangeProposal mcp = new MetadataChangeProposal().setSystemMetadata(systemMetadata);
    OperationContext context =
        TestOperationContexts.systemContextNoValidate().toBuilder()
            .build(new Authentication(new Actor(ActorType.USER, "session"), ""), true);

    recorder.recordIfNeeded(minimalEnvelope(), context, mcp);

    Assert.assertEquals(count.get(), 1);
  }

  @Test
  public void testSwallowsStoreExceptions() {
    UsageQueueIngestRecorder recorder =
        new UsageQueueIngestRecorder(
            new UsageAggregationStore() {
              @Override
              public boolean recordRequest(@Nonnull OperationContext opContext) {
                throw new IllegalStateException("boom");
              }

              @Override
              public void recordResponse(@Nonnull OperationContext opContext, Long outputBytes) {}

              @Override
              public void flush(@Nonnull FlushTrigger trigger) {}
            });
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    OperationContext context =
        TestOperationContexts.systemContextNoValidate().toBuilder()
            .build(new Authentication(new Actor(ActorType.USER, "system"), ""), true);

    recorder.recordIfNeeded(minimalEnvelope(), context, mcp);
  }

  private static InboundMetadataEnvelope<String> minimalEnvelope() {
    return InboundMetadataEnvelope.<String>builder()
        .logicalTopic("MetadataChangeProposal")
        .messagingSystem("kafka")
        .payload("x")
        .enqueuedAtMillis(1L)
        .kafkaPartition(0)
        .kafkaOffset(1L)
        .build();
  }

  private static UsageAggregationStore assertingStore(
      AtomicInteger count, String expectedIdentity) {
    return new UsageAggregationStore() {
      @Override
      public boolean recordRequest(@Nonnull OperationContext opContext) {
        count.incrementAndGet();
        Assert.assertEquals(opContext.getRequestContext().getUsageIdentity(), expectedIdentity);
        return true;
      }

      @Override
      public void recordResponse(@Nonnull OperationContext opContext, Long outputBytes) {}

      @Override
      public void flush(@Nonnull FlushTrigger trigger) {}
    };
  }

  private static UsageAggregationStore simpleCountingStore(AtomicInteger count) {
    return new UsageAggregationStore() {
      @Override
      public boolean recordRequest(@Nonnull OperationContext opContext) {
        count.incrementAndGet();
        return true;
      }

      @Override
      public void recordResponse(@Nonnull OperationContext opContext, Long outputBytes) {}

      @Override
      public void flush(@Nonnull FlushTrigger trigger) {}
    };
  }

  private static UsageAggregationStore countingStore(AtomicInteger count) {
    return new UsageAggregationStore() {
      @Override
      public boolean recordRequest(@Nonnull OperationContext opContext) {
        count.incrementAndGet();
        RequestContext rc = opContext.getRequestContext();
        Assert.assertEquals(rc.getUsageOperation(), "metadata_ingest");
        Assert.assertEquals(rc.getRequestAPI(), RequestContext.RequestAPI.MESSAGING);
        Assert.assertEquals(rc.getUsageIdentity(), "urn:li:corpuser:ingest-actor");
        Assert.assertEquals(rc.getInputBytes(), Long.valueOf(512L));
        return true;
      }

      @Override
      public void recordResponse(@Nonnull OperationContext opContext, Long outputBytes) {}

      @Override
      public void flush(@Nonnull FlushTrigger trigger) {}
    };
  }
}
