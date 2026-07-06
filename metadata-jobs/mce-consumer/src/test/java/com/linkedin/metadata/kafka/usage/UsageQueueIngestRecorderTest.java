package com.linkedin.metadata.kafka.usage;

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
