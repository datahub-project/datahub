package com.linkedin.metadata.config;

/**
 * Backend for {@link SystemMetadataServiceConfig}: Elasticsearch-compatible search, OpenSearch, or
 * PostgreSQL (requires {@code postgres.pgSystemMetadata} SqlSetup). Lowercase values match {@code
 * timeseriesAspectService.implementation}-style configuration elsewhere.
 */
public enum SystemMetadataServiceImplementation {
  elasticsearch,
  opensearch,
  postgres
}
