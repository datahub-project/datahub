package com.linkedin.metadata.datahubusage;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

public interface DataHubUsageService {

  String getUsageIndexName(@Nonnull OperationContext opContext);

  ExternalAuditEventsSearchResponse externalAuditEventsSearch(
      OperationContext opContext,
      ExternalAuditEventsSearchRequest externalAuditEventsSearchRequest);
}
