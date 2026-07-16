package com.linkedin.metadata.datahubusage;

import io.datahubproject.metadata.context.OperationContext;

public interface DataHubUsageService {

  String getUsageIndexName();

  ExternalAuditEventsSearchResponse externalAuditEventsSearch(
      OperationContext opContext,
      ExternalAuditEventsSearchRequest externalAuditEventsSearchRequest);
}
