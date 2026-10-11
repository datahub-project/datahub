package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Optional read-oriented connection pool (same DB split-pool or physical replica). */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ReadPoolConfiguration {
  private boolean enabled;

  /** Ebean JDBC URL; empty defaults to primary {@code ebean.url}. */
  private String url;

  /** Cassandra hosts; empty defaults to primary {@code cassandra.hosts}. */
  private String hosts;

  private String port;
  private String datacenter;
  private long minConnections;
  private long maxConnections;
  private long maxInactiveTimeSeconds;
  private long maxAgeMinutes;
  private long leakTimeMinutes;
  private long waitTimeoutMillis;
}
