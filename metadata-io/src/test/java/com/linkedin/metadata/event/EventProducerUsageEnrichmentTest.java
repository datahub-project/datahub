package com.linkedin.metadata.event;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.usage.UsageDedupHeaders;
import com.linkedin.mxe.DataHubUpgradeHistoryEvent;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EventProducerUsageEnrichmentTest {

  @Test
  public void produceMetadataChangeProposalStampsPreRecordedForAsyncApiIngest() {
    CapturingEventProducer producer = new CapturingEventProducer();
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    OperationContext opContext = org.mockito.Mockito.mock(OperationContext.class);
    RequestContext requestContext =
        RequestContext.builder()
            .actorUrn("urn:li:corpuser:test")
            .sourceIP("127.0.0.1")
            .requestAPI(RequestContext.RequestAPI.OPENAPI)
            .requestID("ingest")
            .userAgent("test")
            .withUsageOperation(UsageOperation.METADATA_INGEST)
            .build();
    org.mockito.Mockito.when(opContext.getRequestContext()).thenReturn(requestContext);

    producer.produceMetadataChangeProposal(
        opContext, UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)"), mcp);

    Assert.assertTrue(UsageDedupHeaders.isPreRecorded(mcp));
    Assert.assertSame(producer.lastMcp, mcp);
  }

  private static final class CapturingEventProducer extends EventProducer {
    private MetadataChangeProposal lastMcp;

    @Override
    public void flush() {}

    @Override
    protected Future<?> doProduceMetadataChangeProposal(
        @Nonnull OperationContext opContext,
        @Nonnull Urn urn,
        @Nonnull MetadataChangeProposal metadataChangeProposal) {
      lastMcp = metadataChangeProposal;
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public Future<?> produceMCL(
        @Nonnull OperationContext opContext,
        @Nonnull Urn urn,
        @Nonnull AspectSpec aspectSpec,
        @Nonnull MetadataChangeLog metadataChangeLog) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public String getMetadataChangeLogTopicName(@Nonnull AspectSpec aspectSpec) {
      return "MCL";
    }

    @Override
    public String getMetadataChangeProposalTopicName() {
      return "MCP";
    }

    @Override
    public Future<?> produceFailedMetadataChangeProposalAsync(
        @Nonnull OperationContext opContext,
        @Nonnull MetadataChangeProposal mcp,
        @Nonnull Set<Throwable> throwables) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public Future<?> producePlatformEvent(
        @Nonnull OperationContext opContext,
        @Nonnull String name,
        @javax.annotation.Nullable String key,
        @Nonnull PlatformEvent payload) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public String getPlatformEventTopicName() {
      return "platform";
    }

    @Override
    public void produceDataHubUpgradeHistoryEvent(
        @Nonnull OperationContext opContext, @Nonnull DataHubUpgradeHistoryEvent event) {}
  }
}
