package com.linkedin.metadata.config;

/**
 * Backend for {@link TimeseriesAspectServiceConfig}: Elasticsearch-compatible search, OpenSearch,
 * or PostgreSQL (requires {@code postgres.pgTimeseries} SqlSetup). Lowercase values match {@code
 * graphService.type}-style configuration elsewhere.
 */
public enum TimeseriesAspectServiceImplementation {
  elasticsearch,
  opensearch,
  postgres
}
