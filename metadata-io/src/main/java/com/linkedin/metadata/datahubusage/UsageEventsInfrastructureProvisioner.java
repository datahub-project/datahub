package com.linkedin.metadata.datahubusage;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * Ensures usage-event storage exists for the configured backend (OpenSearch/Elasticsearch index
 * templates and data streams, or PostgreSQL partitioned tables and partitions).
 */
public interface UsageEventsInfrastructureProvisioner {

  /** Creates or updates indexes/tables/partitions as required for the active implementation. */
  void provision(@Nonnull OperationContext opContext) throws Exception;
}
