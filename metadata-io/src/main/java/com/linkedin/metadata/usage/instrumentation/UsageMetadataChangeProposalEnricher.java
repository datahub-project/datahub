package com.linkedin.metadata.usage.instrumentation;

import com.linkedin.metadata.usage.UsageDedupHeaders;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import javax.annotation.Nonnull;

/**
 * Stamps outbound MCP headers when usage was already recorded at the GMS HTTP entry so the MCE
 * consumer does not double-count async REST ingest.
 */
public final class UsageMetadataChangeProposalEnricher {

  private UsageMetadataChangeProposalEnricher() {}

  public static void enrich(
      @Nonnull final OperationContext context, @Nonnull final MetadataChangeProposal mcp) {
    RequestContext requestContext = context.getRequestContext();
    if (requestContext == null) {
      return;
    }
    if (!UsageOperation.METADATA_INGEST.key().equals(requestContext.getUsageOperation())) {
      return;
    }
    RequestContext.RequestAPI requestApi = requestContext.getRequestAPI();
    if (requestApi != RequestContext.RequestAPI.OPENAPI
        && requestApi != RequestContext.RequestAPI.RESTLI) {
      return;
    }
    UsageDedupHeaders.stampPreRecorded(mcp);
  }
}
