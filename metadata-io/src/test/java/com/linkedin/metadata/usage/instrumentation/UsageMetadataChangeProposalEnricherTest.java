package com.linkedin.metadata.usage.instrumentation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.usage.UsageDedupHeaders;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UsageMetadataChangeProposalEnricherTest {

  @Test
  public void testStampsHeaderForAsyncApiIngest() {
    RequestContext requestContext =
        RequestContext.builder()
            .actorUrn("urn:li:corpuser:test")
            .sourceIP("127.0.0.1")
            .requestAPI(RequestContext.RequestAPI.OPENAPI)
            .requestID("ingest")
            .userAgent("test")
            .withUsageOperation(UsageOperation.METADATA_INGEST)
            .build();
    OperationContext opContext = mock(OperationContext.class);
    when(opContext.getRequestContext()).thenReturn(requestContext);
    MetadataChangeProposal mcp = new MetadataChangeProposal();

    UsageMetadataChangeProposalEnricher.enrich(opContext, mcp);

    Assert.assertTrue(UsageDedupHeaders.isPreRecorded(mcp));
  }

  @Test
  public void testSkipsWhenNotMetadataIngest() {
    RequestContext requestContext =
        RequestContext.builder()
            .actorUrn("urn:li:corpuser:test")
            .sourceIP("127.0.0.1")
            .requestAPI(RequestContext.RequestAPI.OPENAPI)
            .requestID("read")
            .userAgent("test")
            .withUsageOperation(UsageOperation.METADATA_READ)
            .build();
    OperationContext opContext = mock(OperationContext.class);
    when(opContext.getRequestContext()).thenReturn(requestContext);
    MetadataChangeProposal mcp = new MetadataChangeProposal();

    UsageMetadataChangeProposalEnricher.enrich(opContext, mcp);

    Assert.assertFalse(UsageDedupHeaders.isPreRecorded(mcp));
  }

  @Test
  public void testSkipsMessagingRequestApi() {
    RequestContext requestContext =
        RequestContext.builder()
            .actorUrn("urn:li:corpuser:test")
            .sourceIP("queue")
            .requestAPI(RequestContext.RequestAPI.MESSAGING)
            .requestID("mcp")
            .userAgent("datahub/mce-consumer")
            .withUsageOperation(UsageOperation.METADATA_INGEST)
            .build();
    OperationContext opContext = mock(OperationContext.class);
    when(opContext.getRequestContext()).thenReturn(requestContext);
    MetadataChangeProposal mcp = new MetadataChangeProposal();

    UsageMetadataChangeProposalEnricher.enrich(opContext, mcp);

    Assert.assertFalse(UsageDedupHeaders.isPreRecorded(mcp));
  }

  @Test
  public void testPreservesExistingHeaders() {
    RequestContext requestContext =
        RequestContext.builder()
            .actorUrn("urn:li:corpuser:test")
            .sourceIP("127.0.0.1")
            .requestAPI(RequestContext.RequestAPI.RESTLI)
            .requestID("ingest")
            .userAgent("test")
            .withUsageOperation(UsageOperation.METADATA_INGEST)
            .build();
    OperationContext opContext = mock(OperationContext.class);
    when(opContext.getRequestContext()).thenReturn(requestContext);
    MetadataChangeProposal mcp =
        new MetadataChangeProposal().setHeaders(new StringMap(java.util.Map.of("X-Trace", "abc")));

    UsageMetadataChangeProposalEnricher.enrich(opContext, mcp);

    Assert.assertEquals(mcp.getHeaders().get("X-Trace"), "abc");
    Assert.assertTrue(UsageDedupHeaders.isPreRecorded(mcp));
  }
}
